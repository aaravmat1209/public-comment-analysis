import json
import os
import boto3
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from websocket_utils import create_websocket_service

dynamodb = boto3.resource('dynamodb')

def map_state_to_progress(status: str, previous_status: str = None) -> int:
    """Map Step Functions execution status to progress percentage"""
    PROGRESS_MAP = {
        'RUNNING': 50,
        'SUCCEEDED': 100,
        'FAILED': 100,
        'TIMED_OUT': 100,
        'ABORTED': 100
    }
    return PROGRESS_MAP.get(status, 0)

def extract_document_id(execution_input: str) -> Optional[str]:
    """Extract document ID from execution input"""
    try:
        input_data = json.loads(execution_input)
        return input_data.get('documentId')
    except Exception as e:
        print(f"Error extracting document ID from input: {str(e)}")
        return None

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle Step Functions execution status change events"""
    try:
        print(f"Received event: {json.dumps(event)}")
        
        detail = event['detail']
        execution_arn = detail['executionArn']
        status = detail['status']
        
        # Get document ID
        document_id = extract_document_id(detail.get('input', '{}'))
        if not document_id:
            print("Could not extract document ID from execution input")
            return {
                'statusCode': 400,
                'error': 'Missing document ID in execution input'
            }
            
        # Get state table name
        state_table_name = os.environ.get('STATE_TABLE_NAME')
        if not state_table_name:
            print("STATE_TABLE_NAME environment variable not set")
            return {
                'statusCode': 500,
                'error': 'Missing STATE_TABLE_NAME environment variable'
            }
            
        # Get current state from DynamoDB
        state_table = dynamodb.Table(state_table_name)
        response = state_table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': 'metadata'
            }
        )
        
        current_state = json.loads(response['Item']['state']) if 'Item' in response else {}
        previous_status = current_state.get('status')
        
        # Calculate progress
        progress = map_state_to_progress(status, previous_status)
        
        # Create new state
        new_state = {
            'status': status,
            'progress': progress,
            'executionArn': execution_arn,
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        if status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            new_state['error'] = detail.get('cause', 'Execution failed')
            
        # Update state in DynamoDB
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
                ':state': json.dumps(new_state)
            }
        )
        
        # Try to send WebSocket update
        ws_service = create_websocket_service()
        if ws_service:
            try:
                ws_service.broadcast_message({
                    'type': 'PROGRESS_UPDATE',
                    'documentId': document_id,
                    'executionArn': execution_arn,
                    'status': status,
                    'progress': progress,
                    'error': new_state.get('error'),
                    'timestamp': new_state['lastUpdated']
                })
            except Exception as e:
                print(f"Error sending WebSocket update: {str(e)}")
        else:
            print("WebSocket service not available - skipping real-time updates")
        
        return {
            'statusCode': 200,
            'documentId': document_id,
            'status': status,
            'executionArn': execution_arn,
            'progress': progress
        }
        
    except Exception as e:
        print(f"Error handling execution status change: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e)
        }