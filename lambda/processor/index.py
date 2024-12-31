import json
import os
import boto3
import urllib3
import csv
import time
from io import StringIO
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

@dataclass
class Comment:
    """Represents a comment from regulations.gov API"""
    comment_id: str
    agency_id: str
    comment_text: str
    docket_id: str
    document_type: str
    posted_date: str
    last_modified_date: str
    comment_on_document_id: str
    submitter_name: str
    organization: Optional[str]
    submission_type: str
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    duplicate_comments: Optional[int]
    attachments: List[Dict[str, Any]]

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'Comment':
        """Create a Comment instance from API response data"""
        attrs = data['attributes']
        
        # Extract attachments from included data
        attachments = []
        included = data.get('included', [])
        if isinstance(included, list):
            for item in included:
                if item.get('type') == 'attachments':
                    file_formats = item.get('attributes', {}).get('fileFormats', [])
                    if file_formats and isinstance(file_formats, list):
                        attachments.extend(file_formats)

        return cls(
            comment_id=data['id'],
            agency_id=attrs.get('agencyId', ''),
            comment_text=attrs.get('comment', ''),
            docket_id=attrs.get('docketId', ''),
            document_type=attrs.get('documentType', ''),
            posted_date=attrs.get('postedDate', ''),
            last_modified_date=attrs.get('modifyDate', ''),
            comment_on_document_id=attrs.get('commentOnDocumentId', ''),
            submitter_name=attrs.get('submitterName', ''),
            organization=attrs.get('organization'),
            submission_type=attrs.get('subtype', ''),
            first_name=attrs.get('firstName'),
            last_name=attrs.get('lastName'),
            email=attrs.get('email'),
            duplicate_comments=attrs.get('duplicateComments'),
            attachments=attachments
        )

    def to_csv_row(self) -> Dict[str, Any]:
        """Convert comment to CSV-friendly format"""
        data = asdict(self)
        
        # Convert attachments list to string
        data['attachments'] = json.dumps([{
            'fileUrl': att.get('fileUrl', ''),
            'format': att.get('format', ''),
            'size': att.get('size', 0)
        } for att in self.attachments])
        
        # Clean up None values
        for key, value in data.items():
            if value is None:
                data[key] = ''
            
        return data

class RegulationsAPIClient:
    def __init__(self, api_key: str, worker_id: int):
        self.api_key = api_key
        self.base_url = 'https://api.regulations.gov/v4'
        self.http = urllib3.PoolManager()
        self.worker_id = worker_id

    def _make_request(self, path: str, params: Dict[str, str], max_retries: int = 3) -> Dict[str, Any]:
        """Make request to API with retries"""
        url = f"{self.base_url}{path}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        print(f"Worker {self.worker_id} making request to: {url}")
        
        retries = 0
        while retries < max_retries:
            try:
                response = self.http.request(
                    'GET',
                    url,
                    headers={
                        'X-Api-Key': self.api_key,
                        'Accept': 'application/vnd.api+json'
                    }
                )
                
                if response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    retries += 1
                    continue
                    
                if response.status != 200:
                    raise Exception(f"API request failed with status {response.status}: {response.data}")
                    
                return json.loads(response.data.decode('utf-8'))
                
            except urllib3.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {str(e)}")
                if retries < max_retries - 1:
                    wait_time = 2 ** retries  # Exponential backoff
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise
        
    def fetch_comments_page(
        self,
        object_id: str,
        page_number: int,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of comments"""
        params = {
            'filter[commentOnId]': object_id,
            'page[size]': str(page_size),
            'page[number]': str(page_number),
            'sort': 'lastModifiedDate,documentId',
            'include': 'attachments'
        }

        response = self._make_request('/comments', params)
        data = response.get('data', [])
        
        detailed_comments = []
        for comment in data:
            try:
                detailed_comment = self._make_request(f'/comments/{comment["id"]}', {
                    'include': 'attachments'
                })['data']
                detailed_comments.append(detailed_comment)
            except Exception as e:
                print(f"Worker {self.worker_id} error fetching details for comment {comment['id']}: {str(e)}")
                continue
        
        return detailed_comments

def get_secret_value(secret_arn: str) -> str:
    """Retrieve secret value from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    return response['SecretString']

def save_comments_chunk(
    s3_client,
    bucket: str,
    document_id: str,
    worker_id: int,
    page: int,
    comments: List[Comment]
) -> str:
    """Save a page of comments to S3."""
    if not comments:
        return None

    # Create CSV in memory
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=comments[0].to_csv_row().keys())
    writer.writeheader()
    for comment in comments:
        writer.writerow(comment.to_csv_row())

    # Save to S3
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    key = f"comments/{document_id}/worker_{worker_id}/page_{page}_{timestamp}.csv"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )
    
    return key

def update_worker_state(
    dynamodb,
    table_name: str,
    document_id: str,
    worker_id: int,
    work_range: Dict[str, Any],
    current_page: int,
    processed_count: int
) -> None:
    """Update processing state for a worker."""
    try:
        state = {
            'workerId': worker_id,
            'workRange': work_range,
            'currentPage': current_page,
            'processedComments': processed_count,
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        dynamodb.put_item(
            TableName=table_name,
            Item={
                'documentId': {'S': document_id},
                'chunkId': {'S': f'worker_{worker_id}'},
                'state': {'S': json.dumps(state)},
                'ttl': {'N': str(int(time.time() + 7 * 24 * 60 * 60))}
            }
        )
    except Exception as e:
        print(f"Error updating worker state: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process a single page of comments."""
    document_id = event['documentId']
    object_id = event['objectId']
    work_range = event['workRange']['Value'] if 'Value' in event['workRange'] else event['workRange']
    worker_id = work_range['workerId']
    page_number = work_range['pageNumber']
    
    try:
        print(f"Worker {worker_id} processing page {page_number}")
        
        # Initialize clients
        secret_arn = os.environ['REGULATIONS_GOV_API_KEY_SECRET_ARN']
        api_key = get_secret_value(secret_arn)
        
        api_client = RegulationsAPIClient(api_key, worker_id)
        s3_client = boto3.client('s3')
        
        # Fetch the page of comments
        comments_data = api_client.fetch_comments_page(
            object_id,
            page_number,
            work_range['pageSize']
        )
        
        print(f"Worker {worker_id} fetched {len(comments_data)} comments from page {page_number}")
        
        # Process comments
        comments = [Comment.from_api_response(item) for item in comments_data]
        
        # Save to S3
        if comments:
            output_key = f"comments/{document_id}/page_{page_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Save to CSV
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=comments[0].to_csv_row().keys())
            writer.writeheader()
            for comment in comments:
                writer.writerow(comment.to_csv_row())
            
            s3_client.put_object(
                Bucket=os.environ['OUTPUT_S3_BUCKET'],
                Key=output_key,
                Body=csv_buffer.getvalue().encode('utf-8'),
                ContentType='text/csv'
            )
            
            # Save metadata
            metadata_key = f"comments/{document_id}/page_{page_number}_metadata.json"
            metadata = {
                'workerId': worker_id,
                'pageNumber': page_number,
                'processedComments': len(comments),
                'outputFile': output_key,
                'completionTime': datetime.now(timezone.utc).isoformat()
            }
            
            s3_client.put_object(
                Bucket=os.environ['OUTPUT_S3_BUCKET'],
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': len(comments),
                'outputFile': output_key,
                'metadataFile': metadata_key,
                'isComplete': True
            }
        else:
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': 0,
                'isComplete': True
            }

    except Exception as e:
        print(f"Worker {worker_id} encountered an error on page {page_number}: {str(e)}")
        raise