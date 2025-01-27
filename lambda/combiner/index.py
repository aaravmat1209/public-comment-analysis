import json
import os
import boto3
import csv
from io import StringIO
from typing import Dict, List, Any, Tuple
from datetime import datetime, timezone

def get_content_files(
    s3_client, 
    bucket: str, 
    document_id: str,
    content_type: str
) -> List[Dict[str, str]]:
    """Get all files of a specific content type (comments/attachments) for a document in order."""
    content_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f"{document_id}/{content_type}/"
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.csv'):
                    filename = obj['Key'].split('/')[-1]
                    if filename.startswith('worker_'):
                        try:
                            parts = filename.split('_')
                            worker_num = int(parts[1])
                            page_num = int(parts[3])
                            content_files.append({
                                'key': obj['Key'],
                                'worker': worker_num,
                                'page': page_num,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified']
                            })
                        except (IndexError, ValueError):
                            print(f"Skipping file with invalid format: {obj['Key']}")
                            continue
    
    return sorted(content_files, key=lambda x: (x['page'], x['worker']))

def combine_csv_files(
    s3_client, 
    bucket: str, 
    files: List[Dict[str, str]]
) -> Tuple[StringIO, int]:
    """Combine multiple CSV files into one, returning the combined CSV and total rows."""
    combined_csv = StringIO()
    csv_writer = None
    total_rows = 0
    
    print(f"Combining {len(files)} CSV files")
    
    for file_info in files:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=file_info['key'])
            content = response['Body'].read().decode('utf-8')
            file_data = StringIO(content)
            
            reader = csv.DictReader(file_data)
            
            if csv_writer is None:
                csv_writer = csv.DictWriter(combined_csv, fieldnames=reader.fieldnames)
                csv_writer.writeheader()
            
            rows = list(reader)
            total_rows += len(rows)
            for row in rows:
                csv_writer.writerow(row)
            
            print(f"Added {len(rows)} rows from worker {file_info['worker']} page {file_info['page']}")
                
        except Exception as e:
            print(f"Error processing file {file_info['key']}: {str(e)}")
            continue
    
    print(f"Total rows combined: {total_rows}")
    return combined_csv, total_rows

def aggregate_metadata(
    s3_client, 
    bucket: str,
    document_id: str
) -> Dict[str, Any]:
    """Aggregate metadata from all workers."""
    metadata = {
        'totalComments': 0,
        'totalAttachments': 0,
        'totalPages': 0,
        'workerMetadata': [],
        'startTime': None,
        'endTime': None,
        'rateLimitedWorkers': []
    }
    
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f"{document_id}/metadata/"
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.json'):
                    try:
                        response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                        worker_metadata = json.loads(response['Body'].read().decode('utf-8'))
                        metadata['workerMetadata'].append(worker_metadata)
                        metadata['totalComments'] += worker_metadata.get('processedComments', 0)
                        metadata['totalAttachments'] += worker_metadata.get('processedAttachments', 0)
                        metadata['totalPages'] = max(
                            metadata['totalPages'],
                            worker_metadata.get('pageNumber', 0)
                        )
                        
                        if worker_metadata.get('rateLimited', False):
                            metadata['rateLimitedWorkers'].append({
                                'workerId': worker_metadata.get('workerId'),
                                'pageNumber': worker_metadata.get('pageNumber')
                            })
                        
                        completion_time = worker_metadata.get('completionTime')
                        if completion_time:
                            if metadata['startTime'] is None or completion_time < metadata['startTime']:
                                metadata['startTime'] = completion_time
                            if metadata['endTime'] is None or completion_time > metadata['endTime']:
                                metadata['endTime'] = completion_time
                                
                    except Exception as e:
                        print(f"Error reading metadata file {obj['Key']}: {str(e)}")
                        continue
    
    metadata['workerMetadata'].sort(
        key=lambda x: (x.get('pageNumber', 0), x.get('workerId', 0))
    )
    
    return metadata

def create_empty_attachments_file(s3_client, clustering_bucket: str, document_id: str, timestamp: str) -> str:
    """Create an empty attachments CSV file with headers."""
    print(f"Creating empty attachments file for document {document_id}")
    
    # Create empty CSV with headers
    headers = [
        'comment_id', 'document_id', 'attachment_id', 'doc_order',
        'title', 'modify_date', 'file_format', 'file_url', 'size'
    ]
    empty_csv = StringIO()
    writer = csv.writer(empty_csv)
    writer.writerow(headers)
    
    # Save to S3
    attachments_key = f"before-clustering/{document_id}/attachments_{document_id}_{timestamp}.csv"
    s3_client.put_object(
        Bucket=clustering_bucket,
        Key=attachments_key,
        Body=empty_csv.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )
    
    print(f"Created empty attachments file: {attachments_key}")
    return attachments_key

def clean_directory(s3_client, bucket: str, prefix: str) -> None:
    """Delete all files in the specified directory prefix."""
    print(f"Cleaning up directory: {prefix}")
    try:
        # List all objects in the directory
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                # Delete all objects in this page
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={
                            'Objects': objects_to_delete,
                            'Quiet': True
                        }
                    )
                    print(f"Deleted {len(objects_to_delete)} objects from {prefix}")
                    
    except Exception as e:
        print(f"Error cleaning directory {prefix}: {str(e)}")
        # Continue processing even if cleanup fails

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Combine processed comment and attachment files into consolidated files."""
    try:
        document_id = event['documentId']
        processing_results = event.get('processingResults', [])
        
        print(f"Combining results for document {document_id}")
        print(f"Processing results: {json.dumps(processing_results, indent=2)}")
        
        s3_client = boto3.client('s3')
        bucket = os.environ['OUTPUT_S3_BUCKET']
        
        # Clean up the final directory before processing
        final_directory = f"{document_id}/final/"
        clean_directory(s3_client, bucket, final_directory)
        
        # Get all comments and attachments files
        comments_files = get_content_files(s3_client, bucket, document_id, "comments")
        attachments_files = get_content_files(s3_client, bucket, document_id, "attachments")
        
        if not comments_files:
            raise Exception("No comment files found to combine")
            
        print(f"Found {len(comments_files)} comment files and {len(attachments_files)} attachment files to combine")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_files = {}
        
        # Combine comments CSV files
        comments_csv, total_comments = combine_csv_files(s3_client, bucket, comments_files)
        final_comments_key = f"{document_id}/final/comments_{timestamp}.csv"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=final_comments_key,
            Body=comments_csv.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
        final_files['comments'] = final_comments_key
        
        # Copy to clustering bucket if configured
        clustering_bucket = os.environ.get('CLUSTERING_BUCKET')
        print(f"Copying to clustering bucket {clustering_bucket}")
        if clustering_bucket:
            try:
                # Clean up existing files in clustering bucket
                clean_directory(s3_client, clustering_bucket, f"before-clustering/{document_id}/")
                
                # Copy to before-clustering folder in clustering bucket
                clustering_key = f"before-clustering/{document_id}/comments_{document_id}_{timestamp}.csv"
                
                # Copy the combined CSV to the clustering bucket
                s3_client.copy_object(
                    Bucket=clustering_bucket,
                    Key=clustering_key,
                    CopySource={'Bucket': bucket, 'Key': final_comments_key}
                )
                
                print(f"Copied combined CSV to s3://{clustering_bucket}/{clustering_key}")
                final_files['clustering'] = f"s3://{clustering_bucket}/{clustering_key}"
                
                # If no attachments exist, create an empty attachments file
                if not attachments_files:
                    print("No attachments found, creating empty attachments file")
                    attachments_key = create_empty_attachments_file(
                        s3_client,
                        clustering_bucket,
                        document_id,
                        timestamp
                    )
                    final_files['clustering_attachments'] = f"s3://{clustering_bucket}/{attachments_key}"
                
            except Exception as e:
                print(f"Error copying to clustering bucket: {str(e)}")
                # Continue processing even if clustering copy fails
        
        # Combine attachments CSV files if any exist
        total_attachments = 0
        if attachments_files:
            attachments_csv, total_attachments = combine_csv_files(s3_client, bucket, attachments_files)
            final_attachments_key = f"{document_id}/final/attachments_{timestamp}.csv"
            
            s3_client.put_object(
                Bucket=bucket,
                Key=final_attachments_key,
                Body=attachments_csv.getvalue().encode('utf-8'),
                ContentType='text/csv'
            )
            final_files['attachments'] = final_attachments_key
            print(f"Copying to clustering bucket {clustering_bucket}")
            if clustering_bucket:
                try:
                    # Copy to before-clustering folder in clustering bucket
                    attachments_key = f"before-clustering/{document_id}/attachments_{document_id}_{timestamp}.csv"
                    
                    # Copy the combined CSV to the clustering bucket
                    s3_client.copy_object(
                        Bucket=clustering_bucket,
                        Key=attachments_key,
                        CopySource={'Bucket': bucket, 'Key': final_attachments_key}
                    )
                    
                    print(f"Copied combined CSV to s3://{clustering_bucket}/{attachments_key}")
                    final_files['clustering_attachments'] = f"s3://{clustering_bucket}/{attachments_key}"
                except Exception as e:
                    print(f"Error copying to clustering bucket: {str(e)}")
        
        # Aggregate metadata
        metadata = aggregate_metadata(s3_client, bucket, document_id)
        metadata['finalFiles'] = final_files
        metadata['completionTime'] = datetime.now(timezone.utc).isoformat()
        
        # Save final metadata
        metadata_key = f"{document_id}/final/metadata_{timestamp}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        # Optionally cleanup individual files
        if event.get('cleanupFiles', True):
            for file_info in comments_files + attachments_files:
                try:
                    s3_client.delete_object(Bucket=bucket, Key=file_info['key'])
                except Exception as e:
                    print(f"Error deleting file {file_info['key']}: {str(e)}")
            
            # Clean up metadata files
            clean_directory(s3_client, bucket, f"{document_id}/metadata/")
        
        return {
            'documentId': document_id,
            'totalComments': total_comments,
            'totalAttachments': total_attachments,
            'totalPages': metadata['totalPages'],
            'rateLimitedWorkers': metadata['rateLimitedWorkers'],
            'outputFiles': final_files,
            'metadataFile': metadata_key,
            'processingStartTime': metadata['startTime'],
            'processingEndTime': metadata['endTime'],
            'combinedAt': metadata['completionTime']
        }
        
    except Exception as e:
        print(f"Error combining results: {str(e)}")
        raise