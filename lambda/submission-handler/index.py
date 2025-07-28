import json
import os
import boto3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
stepfunctions = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
state_table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized API response without CORS headers"""
    logger.debug(f"Creating response with status {status_code} and body: {body}")
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        },
        'body': json.dumps(body)
    }

def get_analysis_json(document_id: str, cluster_bucket: str) -> Optional[Dict[str, Any]]:
    """Retrieve analysis JSON from clustering bucket if available."""
    try:
        # Look for analysis JSON file
        response = s3_client.list_objects_v2(
            Bucket=cluster_bucket,
            Prefix=f"analysis-json/comments_{document_id}"
        )
        
        if 'Contents' in response:
            # Get the latest analysis file
            latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
            file_content = s3_client.get_object(
                Bucket=cluster_bucket,
                Key=latest_file['Key']
            )
            return json.loads(file_content['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.warning(f"Error retrieving analysis JSON: {str(e)}")
    return None

def submit_document_for_processing(document_id: str) -> Dict[str, Any]:
    """Submit a single document for processing"""
    logger.info(f"Processing submission for document ID: {document_id}")
    
    try:
        # Initialize state in DynamoDB
        current_time = datetime.now(timezone.utc)
        initial_state = {
            'status': 'QUEUED',
            'progress': 0,
            'stage': 'comment_processing',
            'startTime': current_time.isoformat(),
            'lastUpdated': current_time.isoformat()
        }
        
        state_table.put_item(
            Item={
                'documentId': document_id,
                'chunkId': 'metadata',
                'state': json.dumps(initial_state),
                'ttl': int(current_time.timestamp()) + (7 * 24 * 60 * 60)  # 7 days TTL
            }
        )
        logger.info(f"Successfully initialized state for document {document_id}")
        
        # Start Step Functions execution
        execution = stepfunctions.start_execution(
            stateMachineArn=os.environ['STATE_MACHINE_ARN'],
            input=json.dumps({
                'documentId': document_id
            })
        )
        
        logger.info(f"Successfully started execution for document {document_id}")
        logger.debug(f"Execution ARN: {execution['executionArn']}")
        
        return {
            'documentId': document_id,
            'executionArn': execution['executionArn'],
            'status': 'QUEUED'
        }
        
    except Exception as e:
        logger.error(f"Error processing document {document_id}", exc_info=True)
        return {
            'documentId': document_id,
            'error': str(e),
            'status': 'FAILED'
        }

def handle_submission(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle new document submission"""
    logger.info("Processing new document submission")
    
    try:
        body = json.loads(event['body'])
        document_ids = body.get('documentIds', [])
        
        if not document_ids or not isinstance(document_ids, list):
            logger.warning("Invalid submission: Missing or invalid document IDs")
            return create_response(400, {'error': 'Invalid document IDs'})
        
        logger.info(f"Processing submission for {len(document_ids)} documents")
        logger.debug(f"Document IDs: {document_ids}")
        
        results = [submit_document_for_processing(doc_id) for doc_id in document_ids]
        
        successful = len([r for r in results if r['status'] == 'QUEUED'])
        failed = len([r for r in results if r['status'] == 'FAILED'])
        logger.info(f"Submission complete: {successful} successful, {failed} failed")
        
        return create_response(200, {
            'message': 'Processing started',
            'results': results
        })
        
    except json.JSONDecodeError as e:
        logger.error("Failed to parse request body", exc_info=True)
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        logger.error("Unexpected error in submission handler", exc_info=True)
        return create_response(500, {'error': 'Internal server error'})

def check_s3_for_completion(document_id: str) -> bool:
    """Check if the document has completed files in S3."""
    try:
        # Check for final metadata file in S3
        output_bucket = os.environ['OUTPUT_S3_BUCKET']
        response = s3_client.list_objects_v2(
            Bucket=output_bucket,
            Prefix=f"{document_id}/final/"
        )
        
        # If there are files in the final directory, the document is completed
        if 'Contents' in response and len(response['Contents']) > 0:
            logger.info(f"Found completed files for document {document_id} in S3")
            return True
        
        return False
    except Exception as e:
        logger.warning(f"Error checking S3 for completion: {str(e)}")
        return False

def check_clustering_completion(document_id: str, cluster_bucket: str) -> bool:
    """Check if clustering analysis is complete by looking for analysis JSON file."""
    try:
        # Check for analysis JSON file
        response = s3_client.list_objects_v2(
            Bucket=cluster_bucket,
            Prefix=f"analysis-json/comments_{document_id}"
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            logger.info(f"Found analysis JSON for document {document_id}")
            return True
        
        return False
    except Exception as e:
        logger.warning(f"Error checking clustering completion: {str(e)}")
        return False

def check_and_fix_stuck_sagemaker_jobs(document_id: str) -> bool:
    """Check for stuck SageMaker jobs and determine if clustering is actually complete."""
    try:
        # List SageMaker processing jobs for this document
        response = sagemaker_client.list_processing_jobs(
            NameContains=document_id,
            MaxResults=10,
            SortBy='CreationTime',
            SortOrder='Descending'
        )
        
        for job in response.get('ProcessingJobSummaries', []):
            job_name = job['ProcessingJobName']
            job_status = job['ProcessingJobStatus']
            
            logger.info(f"Found SageMaker job {job_name} with status {job_status}")
            
            # If job is stuck in InProgress, check if outputs exist
            if job_status == 'InProgress':
                cluster_bucket = os.environ.get('CLUSTERING_BUCKET')
                if cluster_bucket:
                    # Check if clustered results file exists
                    clustered_results_response = s3_client.list_objects_v2(
                        Bucket=cluster_bucket,
                        Prefix=f"after-clustering/clustered_results_{document_id}"
                    )
                    
                    if 'Contents' in clustered_results_response:
                        logger.info(f"Found clustered results for {document_id}, job likely completed but status not updated")
                        
                        # Try to stop the job to force status update
                        try:
                            sagemaker_client.stop_processing_job(ProcessingJobName=job_name)
                            logger.info(f"Stopped stuck SageMaker job {job_name}")
                        except Exception as stop_error:
                            logger.warning(f"Could not stop job {job_name}: {str(stop_error)}")
                        
                        return True
            
            elif job_status == 'Completed':
                return True
                
        return False
    except Exception as e:
        logger.warning(f"Error checking SageMaker jobs: {str(e)}")
        return False

def handle_status_check(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle document status check with enhanced error handling"""
    document_id = event['pathParameters']['documentId']
    logger.info(f"Checking status for document: {document_id}")
    
    try:
        # Get status from DynamoDB
        response = state_table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            }
        )
        
        if 'Item' not in response:
            logger.warning(f"Document not found: {document_id}")
            return create_response(404, {'error': 'Document not found'})
        
        state = json.loads(response['Item']['state'])
        logger.info(f"Retrieved status for document {document_id}: {state['status']}")
        
        # Check if document is completed in S3 but status is not updated
        if state['status'] != 'SUCCEEDED' and state.get('progress', 0) < 100:
            is_completed = check_s3_for_completion(document_id)
            
            # Also check clustering completion if we have clustering bucket
            cluster_bucket = os.environ.get('CLUSTERING_BUCKET')
            clustering_completed = False
            if cluster_bucket:
                clustering_completed = check_clustering_completion(document_id, cluster_bucket)
                
                # If clustering appears complete but status is stuck, check SageMaker jobs
                if clustering_completed and not is_completed:
                    sagemaker_completed = check_and_fix_stuck_sagemaker_jobs(document_id)
                    if sagemaker_completed:
                        logger.info(f"Document {document_id} clustering is complete, updating status")
                        is_completed = True
            
            if is_completed:
                logger.info(f"Document {document_id} is completed but status is not updated, fixing status")
                state['status'] = 'SUCCEEDED'
                state['progress'] = 100
                state['stage'] = 'completed'
                state['lastUpdated'] = datetime.now(timezone.utc).isoformat()
                
                # Update the state in DynamoDB
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
        
        response_body = {
            'documentId': document_id,
            'documentTitle': state.get('documentTitle', ''),
            'status': state['status'],
            'stage': state.get('stage', 'unknown'),
            'progress': state.get('progress', 0),
            'error': state.get('error'),
            'lastUpdated': state.get('lastUpdated')
        }
        
        # Include updated state for debugging
        response_body['state'] = state
        
        # Get clustering analysis if document is completed successfully OR if analysis exists
        cluster_bucket = os.environ.get('CLUSTERING_BUCKET')
        if cluster_bucket:
            # Try to get analysis if document is completed OR if analysis file exists
            should_get_analysis = (
                (state['status'] in ['SUCCEEDED', 'COMPLETED'] and state.get('progress', 0) >= 100) or
                check_clustering_completion(document_id, cluster_bucket)
            )
            
            if should_get_analysis:
                analysis = get_analysis_json(document_id, cluster_bucket)
                if analysis:
                    response_body['analysis'] = analysis
                    logger.info(f"Successfully retrieved analysis for document {document_id}")
                    
                    # If we found analysis but status isn't complete, update it
                    if state['status'] != 'SUCCEEDED' or state.get('progress', 0) < 100:
                        logger.info(f"Found analysis for {document_id}, updating status to completed")
                        state['status'] = 'SUCCEEDED'
                        state['progress'] = 100
                        state['stage'] = 'completed'
                        state['lastUpdated'] = datetime.now(timezone.utc).isoformat()
                        
                        # Update response body
                        response_body['status'] = state['status']
                        response_body['progress'] = state['progress']
                        response_body['stage'] = state['stage']
                        response_body['lastUpdated'] = state['lastUpdated']
                        
                        # Update the state in DynamoDB
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
                else:
                    response_body['warning'] = 'Analysis results not yet available'
                    logger.warning(f"Analysis results not found for document {document_id}")
        
        # Add failure details if processing failed
        if state['status'] in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            response_body['failureDetails'] = {
                'stage': state.get('stage', 'unknown'),
                'error': state.get('error', 'Unknown error occurred'),
                'failureTime': state.get('lastUpdated')
            }
        
        return create_response(200, response_body)
        
    except Exception as e:
        logger.error(f"Error checking status for document {document_id}", exc_info=True)
        return create_response(500, {
            'error': 'Error checking document status',
            'details': str(e)
        })

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle document submission and status checking"""
    logger.info("Request received")
    
    try:
        if event['httpMethod'] == 'POST':
            return handle_submission(event)
        elif event['httpMethod'] == 'GET':
            return handle_status_check(event)
        else:
            logger.warning(f"Unsupported HTTP method: {event['httpMethod']}")
            return create_response(400, {'error': 'Unsupported method'})
            
    except Exception as e:
        logger.error("Unhandled error in lambda_handler", exc_info=True)
        return create_response(500, {'error': 'Internal server error'})