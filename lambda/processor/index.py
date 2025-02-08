# processor lambda

import json
import os
import boto3
import urllib3
import csv
import time
from io import StringIO
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

class RateLimitReached(Exception):
    """Custom exception for rate limit detection"""
    pass

@dataclass
class AttachmentMetadata:
    """Represents attachment metadata from regulations.gov API"""
    comment_id: str  # Parent comment ID
    document_id: str  # Parent document ID
    attachment_id: str
    doc_order: int
    title: str
    modify_date: str
    file_format: str
    file_url: str
    size: int

    @classmethod
    def from_api_response(
        cls, 
        attachment_data: Dict[str, Any], 
        comment_id: str,
        document_id: str
    ) -> Optional['AttachmentMetadata']:
        """Create AttachmentMetadata from API response"""
        try:
            attrs = attachment_data['attributes']
            file_formats = attrs.get('fileFormats', [])
            
            if not file_formats:
                return None
                
            # Get the first available file format
            file_format = file_formats[0]
            
            return cls(
                comment_id=comment_id,
                document_id=document_id,
                attachment_id=attachment_data['id'],
                doc_order=attrs.get('docOrder', 0),
                title=attrs.get('title', ''),
                modify_date=attrs.get('modifyDate', ''),
                file_format=file_format.get('format', ''),
                file_url=file_format.get('fileUrl', ''),
                size=file_format.get('size', 0)
            )
        except (KeyError, IndexError) as e:
            print(f"Error parsing attachment data: {str(e)}")
            return None

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
 
def format_date_for_api(date_str: str) -> str:
    """Format date string for regulations.gov API.
    Converts ISO format to required format: YYYY-MM-DD HH:mm:ss"""
    if not date_str:
        return None
    try:
        # Parse the ISO format date
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        # Convert to required format
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error formatting date {date_str}: {str(e)}")
        return None

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
        page_size: int = 250,
        last_modified_date: str = None
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of comments with details, handling rate limits and pagination."""
        params = {
            'filter[commentOnId]': object_id,
            'page[size]': str(page_size),
            'page[number]': str(page_number),
            'sort': 'lastModifiedDate,documentId'  # Important for consistent pagination
        }

        # Add last modified date filter if provided
        if last_modified_date:
            formatted_date = format_date_for_api(last_modified_date)
            if formatted_date:
                params['filter[lastModifiedDate][ge]'] = formatted_date
                print(f"Using lastModifiedDate filter: {formatted_date}")

        response = self._make_request('/comments', params)
        data = response.get('data', [])
        
        detailed_comments = []
        last_processed_date = None
        
        for comment in data:
            try:
                # Fetch detailed comment to get the comment text
                detailed_comment = self._make_request(
                    f'/comments/{comment["id"]}',
                    {'include': 'attachments'}
                )
                detailed_comments.append(detailed_comment)
                
                # Track the last modified date for pagination
                comment_date = comment.get('attributes', {}).get('lastModifiedDate')
                if comment_date:
                    if not last_processed_date or comment_date > last_processed_date:
                        last_processed_date = comment_date
                        
            except RateLimitReached:
                print(f"Rate limit reached after processing {len(detailed_comments)} comments")
                break  # Exit but return what we have
            except Exception as e:
                print(f"Error fetching details for comment {comment['id']}: {str(e)}")
                continue
        
        return detailed_comments, last_processed_date

def save_comments_and_attachments(
    s3_client,
    bucket: str,
    document_id: str,
    worker_id: int,
    page: int,
    comments_data: List[Dict[str, Any]],
    rate_limited: bool = False
) -> Tuple[str, Dict[str, Any]]:
    """Save a page of comments to S3 with metadata."""
    if not comments_data:
        return None, None, None
    
    # Process comments
    comments = []
    attachments = []
    
    for comment_response in comments_data:
        comment_data = comment_response['data']
        comment = Comment.from_api_response(comment_data)
        comments.append(comment)
        
        # Process attachments if present in the included data
        if 'included' in comment_response:
            for attachment_data in comment_response['included']:
                if attachment_data['type'] == 'attachments':
                    attachment = AttachmentMetadata.from_api_response(
                        attachment_data,
                        comment.comment_id,
                        document_id
                    )
                    if attachment:
                        attachments.append(attachment)

    # Generate filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    rate_limit_suffix = "_rate_limited" if rate_limited else ""

    # Create CSV in memory
    comments_csv = StringIO()
    if comments:
        writer = csv.DictWriter(comments_csv, fieldnames=comments[0].to_csv_row().keys())
        writer.writeheader()
        for comment in comments:
            writer.writerow(comment.to_csv_row())

    comments_key = f"{document_id}/comments/worker_{worker_id}_page_{page}_{timestamp}{rate_limit_suffix}.csv"
    s3_client.put_object(
        Bucket=bucket,
        Key=comments_key,
        Body=comments_csv.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )

    # Save attachments CSV if we found any
    attachments_key = None
    if attachments:
        attachments_csv = StringIO()
        writer = csv.DictWriter(attachments_csv, fieldnames=asdict(attachments[0]).keys())
        writer.writeheader()
        for attachment in attachments:
            writer.writerow(asdict(attachment))

        attachments_key = f"{document_id}/attachments/worker_{worker_id}_page_{page}_{timestamp}{rate_limit_suffix}.csv"
        s3_client.put_object(
            Bucket=bucket,
            Key=attachments_key,
            Body=attachments_csv.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
    
    # Create metadata
    metadata = {
        'workerId': worker_id,
        'pageNumber': page,
        'processedComments': len(comments),
        'processedAttachments': len(attachments),
        'rateLimited': rate_limited,
        'commentsFile': comments_key,
        'attachmentsFile': attachments_key,
        'completionTime': datetime.now(timezone.utc).isoformat()
    }
    
    # Save metadata
    metadata_key = f"{document_id}/metadata/worker_{worker_id}_page_{page}_{timestamp}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2).encode('utf-8'),
        ContentType='application/json'
    )
    
    return comments_key, attachments_key, metadata

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process a single page of comments with rate limit handling."""
    document_id = event['documentId']
    object_id = event['objectId']
    work_range = event['workRange']['Value'] if 'Value' in event['workRange'] else event['workRange']
    worker_id = work_range['workerId']
    page_number = work_range['pageNumber']
    last_modified_date = event.get('lastModifiedDate')  # Get last modified date if it exists
    
    try:
        print(f"Worker {worker_id} processing page {page_number}")
        if last_modified_date:
            print(f"Using last modified date filter: {last_modified_date}")
        
        # Initialize clients
        secret_arn = os.environ['REGULATIONS_GOV_API_KEY_SECRET_ARN']
        api_key = get_secret_value(secret_arn)
        
        api_client = RegulationsAPIClient(api_key, worker_id)
        s3_client = boto3.client('s3')
        
        rate_limited = False
        comments_data = []
        last_processed_date = None
        
        try:
            # Fetch the page of comments with details
            comments_data, last_processed_date = api_client.fetch_comments_page(
                object_id,
                page_number,
                work_range['pageSize'],
                last_modified_date  # Pass the last modified date to the API
            )
        except RateLimitReached as e:
            print(f"Rate limit reached: {str(e)}")
            rate_limited = True
            
        if page_number != 20:
            last_processed_date = last_modified_date
        
        # Save what we have
        if comments_data:
            comments_key, attachments_key, metadata = save_comments_and_attachments(
                s3_client,
                os.environ['OUTPUT_S3_BUCKET'],
                document_id,
                worker_id,
                page_number,
                comments_data,
                rate_limited
            )
            
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': metadata['processedComments'],
                'processedAttachments': metadata['processedAttachments'],
                'commentsFile': comments_key,
                'attachmentsFile': attachments_key,
                'metadata': metadata,
                'rateLimited': rate_limited,
                'isComplete': not rate_limited,
                'lastProcessedDate': last_processed_date  # Return the last processed date
            }
        else:
            return {
                'workerId': worker_id,
                'pageNumber': page_number,
                'documentId': document_id,
                'processedComments': 0,
                'processedAttachments': 0,
                'rateLimited': rate_limited,
                'isComplete': not rate_limited,
                'lastProcessedDate': last_processed_date
            }

    except Exception as e:
        print(f"Worker {worker_id} encountered an error on page {page_number}: {str(e)}")
        raise