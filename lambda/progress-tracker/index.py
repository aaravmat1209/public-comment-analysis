import json
import os
import boto3
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from websocket_utils import create_websocket_service

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def map_state_to_progress(status: str, stage: str = 'comment_processing') -> int:
    """Map execution status to progress percentage based on pipeline stage"""
    PROGRESS_MAPS = {
        'comment_processing': {
            'RUNNING': 50,
            'SUCCEEDED': 75,
            'FAILED': 75,
            'TIMED_OUT': 75,
            'ABORTED': 75
        },
        'clustering': {
            'RUNNING': 80,
            'SUCCEEDED': 85,
            'FAILED': 85,
            'TIMED_OUT': 85,
            'ABORTED': 85
        },
        'analysis': {
            'RUNNING': 90,
            'SUCCEEDED': 100,
            'FAILED': 100,
            'TIMED_OUT': 100,
            'ABORTED': 100
        }
    }
    return PROGRESS_MAPS.get(stage, {}).get(status, 0)

def extract_document_id(execution_input: str) -> Optional[str]:
    print("Extracting document ID from execution input")
    try:
        input_data = json.loads(execution_input)
        return input_data.get('documentId')
    except Exception as e:
        print(f"Error extracting document ID from input: {str(e)}")
        return None

def update_state(state_table, document_id: str, new_state: Dict[str, Any]) -> None:
    """Update state in DynamoDB with preservation of existing values"""
    try:
        response = state_table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            }
        )
        
        current_state = json.loads(response['Item']['state']) if 'Item' in response else {}
        
        # Merge current state with new state
        merged_state = {**current_state, **new_state}
        
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
        elif event_source == 'aws.sagemaker':
            # SageMaker event
            detail = event['detail']
            status = detail['ProcessingJobStatus']
            document_id = detail['ProcessingJobName'].split('-')[-1]  # Assuming job name contains document ID
            stage = 'clustering'
        else:
            # Direct Lambda invocation (analysis stage)
            document_id = event.get('documentId')
            status = event.get('status', 'SUCCEEDED')
            stage = event.get('stage', 'analysis')
            
        if not document_id:
            logger.error("Could not extract document ID from event")
            return {
                'statusCode': 400,
                'error': 'Missing document ID'
            }
            
        # Calculate progress
        progress = map_state_to_progress(status, stage)
        
        # Create new state
        new_state = {
            'status': status,
            'progress': progress,
            'stage': stage,
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        if stage == 'comment_processing':
            new_state['executionArn'] = execution_arn
            
        if status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            new_state['error'] = event.get('detail', {}).get('cause', 'Execution failed')
        
        # Update state in DynamoDB
        state_table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
        update_state(state_table, document_id, new_state)
        
        # Send WebSocket update
        ws_endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
        api_endpoint = os.environ.get('API_GATEWAY_ENDPOINT')
        connections_table = os.environ.get('CONNECTIONS_TABLE_NAME')

        logger.info(f"Creating WebSocket service with endpoint {api_endpoint or ws_endpoint}")
        
        ws_service = create_websocket_service(
            endpoint=api_endpoint or ws_endpoint,
            connections_table_name=connections_table
        )
        
        if ws_service:
            try:
                ws_service.broadcast_message({
                    'type': 'PROGRESS_UPDATE',
                    'documentId': document_id,
                    'stage': stage,
                    'status': status,
                    'progress': progress,
                    'error': new_state.get('error'),
                    'timestamp': new_state['lastUpdated']
                })
            except Exception as e:
                logger.error(f"Error sending WebSocket update: {str(e)}")
        else:
            logger.warning("WebSocket service not available - skipping real-time updates")
        
        return {
            'statusCode': 200,
            'documentId': document_id,
            'stage': stage,
            'status': status,
            'progress': progress
        }
        
    except Exception as e:
        logger.error(f"Error handling state change: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': str(e)
        }