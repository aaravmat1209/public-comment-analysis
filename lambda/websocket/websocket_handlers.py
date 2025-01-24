import json
import os
import boto3
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table(os.environ['CONNECTIONS_TABLE_NAME'])

def log_websocket_event(event: Dict[str, Any], handler_type: str) -> None:
    """Log details about the incoming WebSocket event"""
    request_context = event.get('requestContext', {})
    connection_id = request_context.get('connectionId', 'Unknown')
    
    logger.info(f"WebSocket {handler_type} Event:")
    logger.info(f"Connection ID: {connection_id}")
    logger.info(f"Event Type: {request_context.get('eventType', 'Unknown')}")
    logger.info(f"Route Key: {request_context.get('routeKey', 'Unknown')}")
    logger.info(f"Domain Name: {request_context.get('domainName', 'Unknown')}")
    logger.info(f"API ID: {request_context.get('apiId', 'Unknown')}")
    
    # Log client identity if available
    identity = request_context.get('identity', {})
    if identity:
        logger.debug("Client Identity Information:")
        logger.debug(f"Source IP: {identity.get('sourceIp', 'Unknown')}")
        logger.debug(f"User Agent: {identity.get('userAgent', 'Unknown')}")

def create_response(status_code: int, body: str) -> Dict[str, Any]:
    """Create standardized WebSocket response"""
    logger.debug(f"Creating response - Status: {status_code}, Body: {body}")
    return {
        'statusCode': status_code,
        'body': body
    }

def store_connection(connection_id: str) -> None:
    """Store connection details in DynamoDB with logging"""
    try:
        logger.debug(f"Storing connection {connection_id} in DynamoDB")
        current_time = datetime.now(timezone.utc)
        
        item = {
            'connectionId': connection_id,
            'timestamp': current_time.isoformat(),
            'ttl': int(current_time.timestamp()) + (2 * 60 * 60)  # 2 hour TTL
        }
        
        logger.debug(f"Connection item to store: {json.dumps(item)}")
        
        connections_table.put_item(Item=item)
        logger.info(f"Successfully stored connection {connection_id}")
        
    except Exception as e:
        logger.error(f"Failed to store connection {connection_id}", exc_info=True)
        raise

def remove_connection(connection_id: str) -> None:
    """Remove connection details from DynamoDB with logging"""
    try:
        logger.debug(f"Removing connection {connection_id} from DynamoDB")
        
        response = connections_table.delete_item(
            Key={
                'connectionId': connection_id
            },
            ReturnValues='ALL_OLD'
        )
        
        if 'Attributes' in response:
            logger.info(f"Successfully removed connection {connection_id}")
            logger.debug(f"Removed connection details: {json.dumps(response['Attributes'])}")
        else:
            logger.warning(f"Connection {connection_id} not found in table")
            
    except Exception as e:
        logger.error(f"Failed to remove connection {connection_id}", exc_info=True)
        raise

def connect_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle new WebSocket connections"""
    logger.info("WebSocket connect handler invoked")
    log_websocket_event(event, "Connect")
    
    try:
        connection_id = event['requestContext']['connectionId']
        logger.info(f"Processing new WebSocket connection: {connection_id}")
        
        # Store connection in DynamoDB
        store_connection(connection_id)
        
        # Get approximate connection count for monitoring
        try:
            count_response = connections_table.scan(
                Select='COUNT'
            )
            total_connections = count_response.get('Count', 0)
            logger.info(f"Total active connections: {total_connections}")
        except Exception as e:
            logger.warning("Failed to get connection count", exc_info=True)
        
        return create_response(200, 'Connected')
        
    except Exception as e:
        logger.error("Error in connect handler", exc_info=True)
        return create_response(500, 'Failed to connect')

def disconnect_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle WebSocket disconnections"""
    logger.info("WebSocket disconnect handler invoked")
    log_websocket_event(event, "Disconnect")
    
    try:
        connection_id = event['requestContext']['connectionId']
        logger.info(f"Processing WebSocket disconnection: {connection_id}")
        
        # Remove connection from DynamoDB
        remove_connection(connection_id)
        
        # Get approximate remaining connection count
        try:
            count_response = connections_table.scan(
                Select='COUNT'
            )
            remaining_connections = count_response.get('Count', 0)
            logger.info(f"Remaining active connections: {remaining_connections}")
        except Exception as e:
            logger.warning("Failed to get connection count", exc_info=True)
        
        return create_response(200, 'Disconnected')
        
    except Exception as e:
        logger.error("Error in disconnect handler", exc_info=True)
        return create_response(500, 'Failed to disconnect')

def default_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle default route for unmatched WebSocket messages"""
    logger.info("WebSocket default handler invoked")
    log_websocket_event(event, "Default")
    
    try:
        connection_id = event['requestContext']['connectionId']
        logger.warning(f"Received unhandled message type from connection {connection_id}")
        
        # Log message body if present
        if 'body' in event:
            try:
                body = json.loads(event['body'])
                logger.debug(f"Unhandled message body: {json.dumps(body)}")
            except json.JSONDecodeError:
                logger.warning("Unable to parse message body as JSON")
                logger.debug(f"Raw message body: {event['body']}")
        
        return create_response(400, 'Unhandled message type')
        
    except Exception as e:
        logger.error("Error in default handler", exc_info=True)
        return create_response(500, 'Internal server error')