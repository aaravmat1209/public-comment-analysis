import json
import os
import boto3
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import urllib3

class WebSocketService:
    def __init__(self, endpoint: Optional[str], connections_table_name: Optional[str]):
        """Initialize WebSocket service with endpoint and table name."""
        if not endpoint:
            raise ValueError("WebSocket endpoint is required")
        if not connections_table_name:
            raise ValueError("Connections table name is required")

        # Convert WebSocket endpoint to HTTPS
        self.endpoint = endpoint.replace('wss://', 'https://')
        self.connections_table = boto3.resource('dynamodb').Table(connections_table_name)
        self.http = urllib3.PoolManager()

    def get_connections(self) -> List[str]:
        """Get all active connection IDs"""
        try:
            response = self.connections_table.scan(
                ProjectionExpression='connectionId'
            )
            return [item['connectionId'] for item in response.get('Items', [])]
        except Exception as e:
            print(f"Error getting connections: {str(e)}")
            return []

    def send_to_connection(self, connection_id: str, data: Dict[str, Any]) -> None:
        """Send message to a specific connection"""
        try:
            url = f"{self.endpoint}/@connections/{connection_id}"
            encoded_data = json.dumps(data).encode('utf-8')
            
            response = self.http.request(
                'POST',
                url,
                body=encoded_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status == 410:  # Gone - connection is stale
                print(f"Connection {connection_id} is stale, removing")
                self.connections_table.delete_item(
                    Key={'connectionId': connection_id}
                )
            elif response.status == 403:  # Permission denied
                print(f"Permission denied sending message to connection {connection_id}. Check IAM permissions.")
                # Delete stale connection since we can't verify it
                self.connections_table.delete_item(
                    Key={'connectionId': connection_id}
                )
            elif response.status != 200:
                print(f"Error sending message to connection {connection_id}: Status {response.status}")
        except Exception as e:
            print(f"Error sending to connection {connection_id}: {str(e)}")

    def broadcast_message(self, data: Dict[str, Any]) -> None:
        """Broadcast message to all connections"""
        try:
            connections = self.get_connections()
            if not connections:
                print("No active connections found")
                return

            print(f"Broadcasting message to {len(connections)} connections")
            for connection_id in connections:
                self.send_to_connection(connection_id, data)
        except Exception as e:
            print(f"Error broadcasting message: {str(e)}")

def create_websocket_service() -> Optional[WebSocketService]:
    """Create WebSocket service from environment variables with error handling."""
    try:
        endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
        connections_table = os.environ.get('CONNECTIONS_TABLE_NAME')
        
        if not endpoint or not connections_table:
            print(f"Missing required environment variables: WEBSOCKET_API_ENDPOINT={endpoint}, CONNECTIONS_TABLE_NAME={connections_table}")
            return None
            
        return WebSocketService(endpoint, connections_table)
    except Exception as e:
        print(f"Error creating WebSocket service: {str(e)}")
        return None