import json
import os
import boto3
import urllib3
import csv
import time
from io import StringIO
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

class RateLimitReached(Exception):
    """Custom exception for rate limit detection"""
    pass

@dataclass
class Comment:
    """Represents a simplified comment from regulations.gov API"""
    comment_id: str
    comment_text: str
    posted_date: str
    last_modified_date: str
    comment_on_document_id: str

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'Comment':
        """Create a Comment instance from API response data"""
        attrs = data['attributes']
        return cls(
            comment_id=data['id'],
            comment_text=attrs.get('comment', ''),
            posted_date=attrs.get('postedDate', ''),
            last_modified_date=attrs.get('modifyDate', ''),
            comment_on_document_id=attrs.get('commentOnDocumentId', '')
        )

    def to_csv_row(self) -> Dict[str, Any]:
        """Convert comment to CSV-friendly format"""
        return asdict(self)

def get_secret_value(secret_arn: str) -> str:
    """Retrieve secret value from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    return response['SecretString']

class RegulationsAPIClient:
    def __init__(self, api_key: str, worker_id: int):
        self.api_key = api_key
        self.base_url = 'https://api.regulations.gov/v4'
        self.http = urllib3.PoolManager()
        self.worker_id = worker_id

    def _make_request(self, path: str, params: Dict[str, str], max_retries: int = 3) -> Dict[str, Any]:
        """Make request to API with rate limit handling"""
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
                
                if response.status == 429:  # Rate limit reached
                    print(f"Worker {self.worker_id} hit rate limit")
                    raise RateLimitReached("API rate limit reached")
                    
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
        
        raise Exception("Max retries exceeded")

    def fetch_comments_page(
        self,
        object_id: str,
        page_number: int,
        page_size: int = 100
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of comments with details, handling rate limits"""
        params = {
            'filter[commentOnId]': object_id,
            'page[size]': str(page_size),
            'page[number]': str(page_number),
            'sort': 'lastModifiedDate,documentId'
        }

        response = self._make_request('/comments', params)
        data = response.get('data', [])
        
        detailed_comments = []
        for comment in data:
            try:
                # Fetch detailed comment to get the comment text
                detailed_comment = self._make_request(f'/comments/{comment["id"]}', {})['data']
                detailed_comments.append(detailed_comment)
            except RateLimitReached:
                # Stop processing more comments but return what we have so far
                print(f"Rate limit reached after processing {len(detailed_comments)} comments")
                raise RateLimitReached(f"Rate limit reached after processing {len(detailed_comments)} comments")
            except Exception as e:
                print(f"Worker {self.worker_id} error fetching details for comment {comment['id']}: {str(e)}")
                continue
        
        return detailed_comments

def save_comments_chunk(
    s3_client,
    bucket: str,
    document_id: str,
    worker_id: int,
    page: int,
    comments: List[Comment],
    rate_limited: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """Save a page of comments to S3 with metadata."""
    if not comments:
        return None, None

    # Create CSV in memory
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=comments[0].to_csv_row().keys())
    writer.writeheader()
    for comment in comments:
        writer.writerow(comment.to_csv_row())

    # Generate filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    rate_limit_suffix = "_rate_limited" if rate_limited else ""
    csv_key = f"comments/{document_id}/worker_{worker_id}/page_{page}_{timestamp}{rate_limit_suffix}.csv"
    metadata_key = f"comments/{document_id}/worker_{worker_id}/page_{page}_{timestamp}_metadata.json"
    
    # Save CSV
    s3_client.put_object(
        Bucket=bucket,
        Key=csv_key,
        Body=csv_buffer.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )
    
    # Create metadata
    metadata = {
        'workerId': worker_id,
        'pageNumber': page,
        'processedComments': len(comments),
        'rateLimited': rate_limited,
        'outputFile': csv_key,
        'completionTime': datetime.now(timezone.utc).isoformat()
    }
    
    # Save metadata
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    
    return csv_key, metadata

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process a single page of comments with rate limit handling."""
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
        
        rate_limited = False
        comments_data = []
        try:
            # Fetch the page of comments with details
            comments_data = api_client.fetch_comments_page(
                object_id,
                page_number,
                work_range['pageSize']
            )
        except RateLimitReached as e:
            print(f"Rate limit reached: {str(e)}")
            rate_limited = True
        
        # Process any comments we got before rate limit
        comments = [Comment.from_api_response(item) for item in comments_data]
        print(f"Worker {worker_id} processed {len(comments)} comments from page {page_number}")
        
        # Save what we have
        if comments:
            output_key, metadata = save_comments_chunk(
                s3_client,
                os.environ['OUTPUT_S3_BUCKET'],
                document_id,
                worker_id,
                page_number,
                comments,
                rate_limited
            )
            
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': len(comments),
                'outputFile': output_key,
                'metadataFile': metadata,
                'rateLimited': rate_limited,
                'isComplete': not rate_limited
            }
        else:
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': 0,
                'rateLimited': rate_limited,
                'isComplete': not rate_limited
            }

    except Exception as e:
        print(f"Worker {worker_id} encountered an error on page {page_number}: {str(e)}")
        raise