import json
import os
import boto3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from websocket_utils import create_websocket_service

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

def extract_document_id(execution_input: str) -> Optional[str]:
    """Extract document ID from Step Functions execution input."""
    logger.info(f"Extracting document ID from input: {execution_input}")
    try:
        input_data = json.loads(execution_input)
        return input_data.get('documentId')
    except Exception as e:
        logger.error(f"Error extracting document ID from input: {str(e)}")
        return None

def map_state_to_progress(status: str, stage: str = 'comment_processing') -> int:
    """Map execution status to progress percentage based on pipeline stage"""
    PROGRESS_MAPS = {
        'comment_processing': {
            'RUNNING': 50,
            'SUCCEEDED': 60,
            'FAILED': 60,
            'TIMED_OUT': 60,
            'ABORTED': 60
        },
        'clustering': {
            'RUNNING': 60,
            'SUCCEEDED': 80,
            'FAILED': 80,
            'TIMED_OUT': 80,
            'ABORTED': 80
        },
        'analysis': {
            'RUNNING': 80,
            'SUCCEEDED': 100,
            'FAILED': 100,
            'TIMED_OUT': 100,
            'ABORTED': 100
        }
    }
    return PROGRESS_MAPS.get(stage, {}).get(status, 0)

def get_error_details(event: Dict[str, Any], stage: str) -> Optional[str]:
    """Extract detailed error information from the event"""
    try:
        if stage == 'comment_processing':
            if 'detail' in event and 'cause' in event['detail']:
                return event['detail']['cause']
            return "Step Functions execution failed"
        elif stage == 'clustering':
            if 'detail' in event and 'FailureReason' in event['detail']:
                return event['detail']['FailureReason']
            return "SageMaker processing job failed"
        elif stage == 'analysis':
            return event.get('error', "Analysis stage failed")
        return "Unknown error occurred"
    except Exception as e:
        logger.error(f"Error extracting error details: {str(e)}")
        return "Failed to extract error details"

def get_current_state(state_table, document_id: str) -> Dict[str, Any]:
    """Get current state from DynamoDB including documentTitle."""
    try:
        response = state_table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            }
        )
        
        if 'Item' in response:
            return json.loads(response['Item']['state'])
        return {}
        
    except Exception as e:
        logger.error(f"Error getting current state: {str(e)}")
        return {}

def update_state(state_table, document_id: str, new_state: Dict[str, Any]) -> None:
    """Update state in DynamoDB with preservation of existing values"""
    try:
        # Get current state to preserve documentTitle and other fields
        current_state = get_current_state(state_table, document_id)
        logger.info(f"Current state: {json.dumps(current_state)}")
        
        # Preserve documentTitle from current state if it exists
        if 'documentTitle' in current_state and 'documentTitle' not in new_state:
            new_state['documentTitle'] = current_state['documentTitle']
            
        # Merge current state with new state, preserving error information
        if new_state.get('status') in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            if 'error' not in new_state and 'error' in current_state:
                new_state['error'] = current_state['error']
                
        merged_state = {**current_state, **new_state}
        logger.info(f"Merged state: {json.dumps(merged_state)}")
        
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
                ':state': json.dumps(merged_state)
            }
        )
        logger.info(f"Updated state for document {document_id}: {merged_state}")
        
    except Exception as e:
        logger.error(f"Error updating state: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle state changes from Step Functions, SageMaker, and Lambda events"""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Determine event type and extract relevant information
        event_source = event.get('source', '')
        stage = 'comment_processing'  # Default stage
        
        if event_source == 'aws.states':
            # Step Functions event
            detail = event['detail']
            execution_arn = detail['executionArn']
            status = detail['status']
            document_id = extract_document_id(detail.get('input', '{}'))
            stage = 'comment_processing'
            logger.info(f"Step Functions state change - Status: {status}, Document: {document_id}")
        elif event_source == 'aws.sagemaker':
            # SageMaker event
            detail = event['detail']
            status = detail['ProcessingJobStatus']
            document_id = detail['ProcessingJobName'].split('-')[-1]
            stage = 'clustering'
            logger.info(f"SageMaker status change - Status: {status}, Document: {document_id}")
        else:
            # Direct Lambda invocation (analysis stage)
            document_id = event.get('documentId')
            status = event.get('status', 'SUCCEEDED')
            stage = event.get('stage', 'analysis')
            logger.info(f"Analysis status change - Status: {status}, Document: {document_id}")
            
        if not document_id:
            logger.error("Could not extract document ID from event")
            return {
                'statusCode': 400,
                'error': 'Missing document ID'
            }
            
        # Calculate progress
        progress = map_state_to_progress(status, stage)
        logger.info(f"Calculated progress: {progress}% for stage: {stage}, status: {status}")
        
        # Get current state to retrieve documentTitle
        state_table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
        current_state = get_current_state(state_table, document_id)
        
        # Create new state
        new_state = {
            'status': status,
            'progress': progress,
            'stage': stage,
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        if stage == 'comment_processing':
            new_state['executionArn'] = execution_arn
            
        # Preserve documentTitle if it exists
        if 'documentTitle' in current_state:
            new_state['documentTitle'] = current_state['documentTitle']
            
        if status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            error_details = get_error_details(event, stage)
            new_state['error'] = error_details
            logger.error(f"Processing failed in {stage} stage: {error_details}")
        
        # Update state in DynamoDB
        update_state(state_table, document_id, new_state)
        
        # Send WebSocket update with enhanced error information
        ws_endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
        api_endpoint = os.environ.get('API_GATEWAY_ENDPOINT')
        connections_table = os.environ.get('CONNECTIONS_TABLE_NAME')
        
        ws_service = create_websocket_service(
            endpoint=api_endpoint or ws_endpoint,
            connections_table_name=connections_table
        )
        
        if ws_service:
            try:
                message = {
                    'type': 'PROGRESS_UPDATE',
                    'documentId': document_id,
                    'stage': stage,
                    'status': status,
                    'progress': progress,
                    'documentTitle': new_state.get('documentTitle', ''),
                    'error': new_state.get('error'),
                    'timestamp': new_state['lastUpdated']
                }
                logger.info(f"Sending WebSocket message: {json.dumps(message)}")
                ws_service.broadcast_message(message)
            except Exception as e:
                logger.error(f"Error sending WebSocket update: {str(e)}")
        else:
            logger.warning("WebSocket service not available - skipping real-time updates")
        
        return {
            'statusCode': 200,
            'documentId': document_id,
            'stage': stage,
            'status': status,
            'progress': progress,
            'documentTitle': new_state.get('documentTitle', ''),
            'error': new_state.get('error')
        }
        
    except Exception as e:
        logger.error(f"Error handling state change: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': str(e)
        }