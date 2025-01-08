import json
import os
import boto3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
stepfunctions = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
state_table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized API response with CORS headers"""
    logger.debug(f"Creating response with status {status_code} and body: {body}")
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }

def log_request_details(event: Dict[str, Any]) -> None:
    """Log detailed information about the incoming request"""
    logger.info("Request Details:")
    logger.info(f"Method: {event.get('httpMethod')}")
    logger.info(f"Path: {event.get('path')}")
    logger.info(f"Headers: {json.dumps(event.get('headers', {}))}")
    logger.info(f"Query Parameters: {json.dumps(event.get('queryStringParameters', {}))}")
    
    # Log request body if present, safely handling potential JSON parsing errors
    if 'body' in event:
        try:
            body = json.loads(event['body'])
            logger.info(f"Request Body: {json.dumps(body)}")
        except json.JSONDecodeError:
            logger.warning("Unable to parse request body as JSON")
            logger.info(f"Raw Body: {event['body']}")

def submit_document_for_processing(document_id: str) -> Dict[str, Any]:
    """Submit a single document for processing"""
    logger.info(f"Processing submission for document ID: {document_id}")
    
    try:
        # Log DynamoDB operation start
        logger.debug(f"Initializing state in DynamoDB for document {document_id}")
        current_time = datetime.now(timezone.utc)
        
        # Initialize state in DynamoDB
        state_table.put_item(
            Item={
                'documentId': document_id,
                'chunkId': 'metadata',
                'state': json.dumps({
                    'status': 'QUEUED',
                    'progress': 0,
                    'startTime': current_time.isoformat(),
                    'lastUpdated': current_time.isoformat()
                }),
                'ttl': int(current_time.timestamp()) + (7 * 24 * 60 * 60)  # 7 days TTL
            }
        )
        logger.info(f"Successfully initialized state for document {document_id}")
        
        # Log Step Functions execution start
        logger.debug(f"Starting Step Functions execution for document {document_id}")
        
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
        
        # Log submission results
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

def handle_status_check(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle document status check"""
    document_id = event['pathParameters']['documentId']
    logger.info(f"Checking status for document: {document_id}")
    
    try:
        # Log DynamoDB operation start
        logger.debug(f"Retrieving state from DynamoDB for document {document_id}")
        
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
        logger.debug(f"Full state: {json.dumps(state)}")
        
        return create_response(200, {
            'documentId': document_id,
            'status': state
        })
        
    except Exception as e:
        logger.error(f"Error checking status for document {document_id}", exc_info=True)
        return create_response(500, {'error': 'Error checking document status'})

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle document submission and status checking"""
    # Log initial request details
    logger.info("Request received")
    log_request_details(event)
    
    try:
        # Handle different HTTP methods
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