import json
import os
import boto3
import csv
from io import StringIO
from typing import Dict, List, Any
from datetime import datetime, timezone

def get_page_files(
    s3_client, 
    bucket: str, 
    document_id: str
) -> List[Dict[str, str]]:
    """Get all page files for a document in order."""
    page_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f"comments/{document_id}/"
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.csv'):
                    # Extract page number from filename
                    filename = obj['Key'].split('/')[-1]
                    if filename.startswith('page_'):
                        try:
                            page_num = int(filename.split('_')[1])
                            page_files.append({
                                'key': obj['Key'],
                                'page': page_num,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified']
                            })
                        except (IndexError, ValueError):
                            print(f"Skipping file with invalid format: {obj['Key']}")
                            continue
    
    # Sort by page number
    return sorted(page_files, key=lambda x: x['page'])

def combine_page_files(
    s3_client, 
    bucket: str, 
    page_files: List[Dict[str, str]]
) -> StringIO:
    """Combine multiple page files into one CSV."""
    combined_csv = StringIO()
    csv_writer = None
    total_comments = 0
    
    print(f"Combining {len(page_files)} page files")
    
    for file_info in page_files:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=file_info['key'])
            content = response['Body'].read().decode('utf-8')
            page_data = StringIO(content)
            
            # Read page CSV
            reader = csv.DictReader(page_data)
            
            # Initialize writer with headers from first file
            if csv_writer is None:
                csv_writer = csv.DictWriter(combined_csv, fieldnames=reader.fieldnames)
                csv_writer.writeheader()
            
            # Write rows from this page
            rows = list(reader)
            total_comments += len(rows)
            for row in rows:
                csv_writer.writerow(row)
            
            print(f"Added {len(rows)} comments from page {file_info['page']}")
                
        except Exception as e:
            print(f"Error processing page file {file_info['key']}: {str(e)}")
            continue
    
    print(f"Total comments combined: {total_comments}")
    return combined_csv

def aggregate_metadata(
    s3_client, 
    bucket: str,
    document_id: str
) -> Dict[str, Any]:
    """Aggregate metadata from all pages."""
    metadata = {
        'totalComments': 0,
        'totalPages': 0,
        'pageMetadata': [],
        'startTime': None,
        'endTime': None
    }
    
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f"comments/{document_id}/"
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('_metadata.json'):
                    try:
                        response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                        page_metadata = json.loads(response['Body'].read().decode('utf-8'))
                        metadata['pageMetadata'].append(page_metadata)
                        metadata['totalComments'] += page_metadata.get('processedComments', 0)
                        metadata['totalPages'] += 1
                        
                        # Track start and end times
                        completion_time = page_metadata.get('completionTime')
                        if completion_time:
                            if metadata['startTime'] is None or completion_time < metadata['startTime']:
                                metadata['startTime'] = completion_time
                            if metadata['endTime'] is None or completion_time > metadata['endTime']:
                                metadata['endTime'] = completion_time
                                
                    except Exception as e:
                        print(f"Error reading metadata file {obj['Key']}: {str(e)}")
                        continue
    
    # Sort page metadata by page number
    metadata['pageMetadata'].sort(key=lambda x: x.get('pageNumber', 0))
    
    return metadata

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Combine processed comment pages into a single file."""
    try:
        document_id = event['documentId']
        processing_results = event.get('processingResults', [])
        
        print(f"Combining results for document {document_id}")
        print(f"Processing results: {json.dumps(processing_results, indent=2)}")
        
        s3_client = boto3.client('s3')
        bucket = os.environ['OUTPUT_S3_BUCKET']
        
        # Get all page files
        page_files = get_page_files(s3_client, bucket, document_id)
        
        if not page_files:
            raise Exception("No page files found to combine")
            
        print(f"Found {len(page_files)} page files to combine")
        
        # Combine CSV files
        combined_csv = combine_page_files(s3_client, bucket, page_files)
        
        # Save combined CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_csv_key = f"comments/{document_id}/final_comments_{timestamp}.csv"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=final_csv_key,
            Body=combined_csv.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
        
        # Aggregate metadata
        metadata = aggregate_metadata(s3_client, bucket, document_id)
        metadata['finalFile'] = final_csv_key
        metadata['completionTime'] = datetime.now(timezone.utc).isoformat()
        
        # Save final metadata
        metadata_key = f"comments/{document_id}/final_metadata_{timestamp}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        # Optionally cleanup individual page files
        if event.get('cleanupFiles', True):
            for file_info in page_files:
                try:
                    s3_client.delete_object(Bucket=bucket, Key=file_info['key'])
                    # Also delete corresponding metadata file
                    metadata_key = file_info['key'].replace('.csv', '_metadata.json')
                    s3_client.delete_object(Bucket=bucket, Key=metadata_key)
                except Exception as e:
                    print(f"Error deleting file {file_info['key']}: {str(e)}")
        
        return {
            'documentId': document_id,
            'totalComments': metadata['totalComments'],
            'totalPages': metadata['totalPages'],
            'outputFile': final_csv_key,
            'metadataFile': metadata_key,
            'processingStartTime': metadata['startTime'],
            'processingEndTime': metadata['endTime'],
            'combinedAt': metadata['completionTime']
        }
        
    except Exception as e:
        print(f"Error combining results: {str(e)}")
        raise