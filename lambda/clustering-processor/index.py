import json
import boto3
import os
import uuid
import logging
from datetime import datetime, timezone
from websocket_utils import create_websocket_service

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_progress_update(document_id: str) -> None:
    """Send clustering progress update via WebSocket"""
    try:
        # Initialize WebSocket service
        ws_endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
        api_endpoint = os.environ.get('API_GATEWAY_ENDPOINT')
        connections_table = os.environ.get('CONNECTIONS_TABLE_NAME')

        ws_service = create_websocket_service(
            endpoint=api_endpoint or ws_endpoint,
            connections_table_name=connections_table
        )
        
        if ws_service:
            ws_service.broadcast_message({
                'type': 'PROGRESS_UPDATE',
                'documentId': document_id,
                'stage': 'clustering',
                'status': 'RUNNING',
                'progress': 60,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
    except Exception as e:
        logger.error(f"Error sending WebSocket update: {str(e)}")

def update_processing_state(document_id: str, status: str, error: str = None) -> None:
    """Update processing state in DynamoDB"""
    try:
        dynamodb = boto3.resource('dynamodb')
        state_table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
        
        # Get current state to preserve existing values
        response = state_table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            }
        )
        
        current_state = {}
        if 'Item' in response:
            current_state = json.loads(response['Item']['state'])
        
        # Only update if we're not already completed
        if current_state.get('stage') == 'completed':
            logger.info(f"Document {document_id} already completed, skipping clustering status update")
            return
            
        state = {
            **current_state,
            'status': status,
            'stage': 'clustering',
            'progress': 80 if status == 'RUNNING' else 85,  # Keep at 85% for clustering completion
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        if error:
            state['error'] = error
            
        state_table.update_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            },
            UpdateExpression='SET #state = :state',
            ExpressionAttributeNames={
                '#state': 'state'
            },
            ExpressionAttributeValues={
                ':state': json.dumps(state)
            }
        )
    except Exception as e:
        logger.error(f"Error updating state: {str(e)}")
        
def create_job_name(document_id: str) -> str:
    """Create a SageMaker job name that respects length limits."""
    # Maximum length for SageMaker job names is 63 characters
    max_length = 63
    prefix = "clustering-"
    uuid_length = 8  # We'll use a shorter UUID
    
    # Calculate how much space we have for the document ID
    available_space = max_length - len(prefix) - uuid_length - 1  # -1 for the hyphen
    
    # Truncate document ID if necessary
    if len(document_id) > available_space:
        document_id = document_id[:available_space]
    
    # Create a shorter unique identifier
    unique_id = str(uuid.uuid4())[:uuid_length]
    
    return f"{prefix}{document_id}-{unique_id}"

def extract_doc_id_from_path(s3_key: str):
    """Extract document ID from S3 key with directory structure."""
    # Key format: before-clustering/EPA-R10-OW-2017-0369-0001/comments_EPA-R10-OW-2017-0369-0001_...csv
    parts = s3_key.split('/')
    if len(parts) >= 2:
        return parts[1]  # The document ID is the directory name
    return None

def verify_files_exist(s3_client, bucket: str, doc_id: str) -> bool:
    """Verify that both comments and attachments files exist for the document."""
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"before-clustering/{doc_id}/"
    )
    
    if 'Contents' not in response:
        logger.warning(f"No files found for document {doc_id}")
        return False
        
    files = [obj['Key'] for obj in response.get('Contents', [])]
    has_comments = any('comments_' in f for f in files)
    has_attachments = any('attachments_' in f for f in files)
    
    logger.info(f"Files found for {doc_id} - Comments: {has_comments}, Attachments: {has_attachments}")
    return has_comments and has_attachments

def lambda_handler(event, context):
    """Start a SageMaker processing job for clustering"""
    sagemaker_client = boto3.client('sagemaker')
    s3_client = boto3.client('s3')
    
    try:
        records = event.get('Records', [])
            
        if not records:
            logger.error("No records found in the event.")
            return {
                'statusCode': 400,
                'body': json.dumps('No records found in the event.')
            }

        for record in records:
            # Get the S3 bucket and object key
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')
            
            if not bucket or not key:
                logger.error("Bucket or key not found in the event.")
                continue

            # Skip if not a comments file
            if not 'comments_' in key:
                logger.info(f"Skipping non-comments file: {key}")
                continue

            # Extract document ID from key
            document_id = extract_doc_id_from_path(key)
            if not document_id:
                logger.error(f"Could not extract document ID from path: {key}")
                continue

            # Verify both files exist
            if not verify_files_exist(s3_client, bucket, document_id):
                logger.info(f"Still waiting for all files for document {document_id}")
                return {
                    'statusCode': 200,
                    'body': json.dumps('Waiting for all files to be uploaded')
                }
            
            # Send initial progress update
            send_progress_update(document_id)
            update_processing_state(document_id, 'RUNNING')

            logger.info(f"Processing clustering for document {document_id}")
            logger.info(f"Input directory: s3://{bucket}/before-clustering/{document_id}/")

            # Prepare SageMaker job
            job_name = create_job_name(document_id)
            input_s3_uri = f"s3://{bucket}/before-clustering/{document_id}"
            output_s3_uri = f"s3://{bucket}/after-clustering/"

            # Start SageMaker processing job
            response = sagemaker_client.create_processing_job(
                ProcessingJobName=job_name,
                ProcessingResources={
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': 'ml.c5.xlarge',
                        'VolumeSizeInGB': 30
                    }
                },
                StoppingCondition={
                    'MaxRuntimeInSeconds': 3600
                },
                AppSpecification={
                    'ImageUri': os.environ['IMAGE_URI'],
                    'ContainerEntrypoint': [
                        "python3",
                        "/opt/ml/processing/input/code/processing_script.py"
                    ],
                    'ContainerArguments': [
                        "--input-data", "/opt/ml/processing/input/data",
                        "--output-data", "/opt/ml/processing/output",
                        "--doc-id", document_id,
                        "--n-clusters", "10",
                    ]
                },
                ProcessingInputs=[
                    {
                        'InputName': 'input-data',
                        'S3Input': {
                            'S3Uri': f"s3://{bucket}/before-clustering",
                            'LocalPath': '/opt/ml/processing/input/data',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File'
                        }
                    },
                    {
                        'InputName': 'code',
                        'S3Input': {
                            'S3Uri': f"s3://{bucket}/process/processing_script.py",
                            'LocalPath': '/opt/ml/processing/input/code',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File'
                        }
                    }
                ],
                ProcessingOutputConfig={
                    'Outputs': [
                        {
                            'OutputName': 'output-data',
                            'S3Output': {
                                'S3Uri': output_s3_uri,
                                'LocalPath': '/opt/ml/processing/output',
                                'S3UploadMode': 'EndOfJob'
                            }
                        }
                    ]
                },
                Tags=[{
                    'Key': 'DocumentId',
                    'Value': document_id
                }],
                RoleArn=os.environ['ROLE_ARN']
            )

            logger.info(f"Started SageMaker processing job: {job_name}")
            
            # Update state to indicate clustering is in progress
            update_processing_state(document_id, 'RUNNING')

        return {
            'statusCode': 200,
            'body': json.dumps('Processing jobs started successfully.')
        }

    except Exception as e:
        logger.error(f"Error starting processing job: {str(e)}")
        if 'document_id' in locals():
            update_processing_state(document_id, 'FAILED', str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error starting processing job: {str(e)}")
        }