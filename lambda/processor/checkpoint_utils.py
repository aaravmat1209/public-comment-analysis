import boto3
import json
import os
from datetime import datetime, timezone

def get_checkpoint(document_id, worker_id, page_number):
    """Get the last checkpoint for a specific work range."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
    
    checkpoint_id = f"checkpoint_{worker_id}_{page_number}"
    
    try:
        response = table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': checkpoint_id
            }
        )
        
        if 'Item' in response:
            return json.loads(response['Item']['checkpoint'])
        return None
    except Exception as e:
        print(f"Error retrieving checkpoint: {str(e)}")
        return None

def save_checkpoint(document_id, worker_id, page_number, checkpoint_data):
    """Save a checkpoint for a specific work range."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
    
    checkpoint_id = f"checkpoint_{worker_id}_{page_number}"
    
    try:
        table.put_item(
            Item={
                'documentId': document_id,
                'chunkId': checkpoint_id,
                'checkpoint': json.dumps(checkpoint_data),
                'lastUpdated': datetime.now(timezone.utc).isoformat(),
                'ttl': int(datetime.now(timezone.utc).timestamp() + (7 * 24 * 60 * 60))  # 7 days TTL
            }
        )
        return True
    except Exception as e:
        print(f"Error saving checkpoint: {str(e)}")
        return False