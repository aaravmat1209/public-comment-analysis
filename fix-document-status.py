import boto3
import json
from datetime import datetime, timezone

def update_document_status(document_id, state_table_name):
    """Update document status to SUCCEEDED with 100% progress."""
    try:
        # Update the document status in DynamoDB
        dynamodb = boto3.resource('dynamodb')
        state_table = dynamodb.Table(state_table_name)
        
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
        
        # Create new state with SUCCEEDED status and 100% progress
        new_state = {
            **current_state,
            'status': 'SUCCEEDED',  # This is the key change - update status from QUEUED to SUCCEEDED
            'progress': 100,        # Update progress to 100%
            'stage': 'completed',   # Set stage to completed
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
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
        
        print(f"Updated document status to SUCCEEDED with 100% progress")
        return True
        
    except Exception as e:
        print(f"Error updating document status: {str(e)}")
        return False

if __name__ == "__main__":
    # Replace these with your actual values
    document_id = "EPA-HQ-OAR-2025-0124-0001"
    state_table_name = "PublicCommentAnalysisStack-ProcessingStateTable27C46B9A-NVI7C41QJDXM"
    
    success = update_document_status(document_id, state_table_name)
    if success:
        print(f"Successfully updated status for document {document_id}")
    else:
        print(f"Failed to update status for document {document_id}")