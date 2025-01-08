# initializer lambda

import json
import os
import boto3
import urllib3
from typing import Dict, Any
from datetime import datetime, timezone
from botocore.exceptions import ClientError

class RegulationsAPIClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = 'https://api.regulations.gov/v4'
        self.http = urllib3.PoolManager()

    def _make_request(self, path: str, params: Dict[str, str]) -> Dict[str, Any]:
        """Make request to API with retries"""
        url = f"{self.base_url}{path}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        print(f"Making request to: {url}")
        
        response = self.http.request(
            'GET',
            url,
            headers={
                'X-Api-Key': self.api_key,
                'Accept': 'application/vnd.api+json'
            }
        )
        
        if response.status != 200:
            raise Exception(f"API request failed with status {response.status}: {response.data}")
            
        return json.loads(response.data.decode('utf-8'))

    def get_document_info(self, document_id: str) -> Dict[str, Any]:
        """Get document metadata and comment count."""
        print(f"Fetching document info for: {document_id}")
        
        # Get document object ID
        document_response = self._make_request(f'/documents/{document_id}', {})
        object_id = document_response['data']['attributes']['objectId']
        
        # Get total comment count
        comments_response = self._make_request('/comments', {
            'filter[commentOnId]': object_id,
            'page[size]': '10',
            'page[number]': '1'
        })
        
        total_comments = comments_response.get('meta', {}).get('totalElements', 0)
        print(f"Found {total_comments} total comments")
        
        return {
            'objectId': object_id,
            'totalComments': total_comments,
            'document': document_response['data']
        }

def get_secret_value(secret_arn: str) -> str:
    """Retrieve secret value from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        return response['SecretString']
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        raise

def initialize_state(
    dynamodb,
    table_name: str,
    document_id: str,
    document_info: Dict[str, Any]
) -> Dict[str, Any]:
    """Initialize processing state in DynamoDB."""
    total_comments = min(document_info['totalComments'], 100)
    state = {
        'documentId': document_id,
        'objectId': document_info['objectId'],
        'totalComments': total_comments,
        'processedComments': 0,
        'lastProcessedPage': 0,
        'startTime': datetime.now(timezone.utc).isoformat(),
        'status': 'INITIALIZED',
        'ttl': int((datetime.now(timezone.utc).timestamp() + (7 * 24 * 60 * 60)))  # 7 days TTL
    }
    
    try:
        dynamodb.put_item(
            TableName=table_name,
            Item={
                'documentId': {'S': document_id},
                'chunkId': {'S': 'metadata'},
                'state': {'S': json.dumps(state)},
                'ttl': {'N': str(state['ttl'])}
            }
        )
    except ClientError as e:
        print(f"Error saving state to DynamoDB: {str(e)}")
        raise
    
    return state

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Initialize the comment processing."""
    try:
        document_id = event['documentId']
        print(f"Initializing processing for document: {document_id}")
        
        # Get API key from Secrets Manager
        secret_arn = os.environ['REGULATIONS_GOV_API_KEY_SECRET_ARN']
        api_key = get_secret_value(secret_arn)
        
        # Initialize API client
        api_client = RegulationsAPIClient(api_key)
        
        # Get document information and comment count
        document_info = api_client.get_document_info(document_id)
        total_comments = min(document_info['totalComments'], 100)
        print(f"Found {total_comments} comments for document")
        
        # Initialize state in DynamoDB
        dynamodb = boto3.client('dynamodb')
        state = initialize_state(
            dynamodb,
            os.environ['STATE_TABLE_NAME'],
            document_id,
            document_info
        )
        
        return {
            'documentId': document_id,
            'objectId': document_info['objectId'],
            'totalComments': total_comments,
            'startTime': state['startTime']
        }

    except Exception as e:
        print(f"Error in initializer: {str(e)}")
        raise