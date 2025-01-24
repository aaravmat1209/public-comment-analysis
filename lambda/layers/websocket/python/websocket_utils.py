import json
import os
import boto3
import urllib3
from typing import Dict, Any, List, Optional
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from urllib.parse import urlparse

class WebSocketService:
    def __init__(self, endpoint: str, connections_table_name: str):
        """Initialize WebSocket service with endpoint and table name."""
        if not endpoint:
            raise ValueError("WebSocket endpoint is required")
        if not connections_table_name:
            raise ValueError("Connections table name is required")

        print(f"Converting WebSocket endpoint to HTTPS...")
        self.endpoint = endpoint.replace('wss://', 'https://')
        self.connections_table = boto3.resource('dynamodb').Table(connections_table_name)
        self.http = urllib3.PoolManager(
            retries=urllib3.Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        
        # Get credentials from boto3 session
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        self.region = self.session.region_name or 'us-west-2'

    def _sign_request(self, method: str, url: str, body: str = '') -> Dict[str, str]:
        """Sign request with AWS SigV4."""
        parsed_url = urlparse(url)
        request = AWSRequest(
            method=method,
            url=url,
            data=body,
            headers={
                'Content-Type': 'application/json',
                'host': parsed_url.netloc
            }
        )
        
        credentials = self.credentials.get_frozen_credentials()
        sigv4 = SigV4Auth(credentials, 'execute-api', self.region)
        sigv4.add_auth(request)
        
        return dict(request.headers)

    def get_connections(self) -> list:
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
        print(f"Sending message to a connection with id {connection_id}...")
        try:
            url = f"{self.endpoint}/@connections/{connection_id}"
            print(f"Sending message to endpoint {url}")
            encoded_data = json.dumps(data).encode('utf-8')
            
            # Sign the request
            headers = self._sign_request('POST', url, encoded_data.decode('utf-8'))
            
            response = self.http.request(
                'POST',
                url,
                body=encoded_data,
                headers=headers,
            )
            
            print(f"Response status: {response.status}")
            
            if response.status == 410:  # Gone - connection is stale
                print(f"Connection {connection_id} is stale, removing")
                self.connections_table.delete_item(
                    Key={'connectionId': connection_id}
                )
            elif response.status == 403:  # Permission denied
                print(f"Permission denied sending message to connection {connection_id}. URL: {url}")
                print(f"Response: {response.data.decode('utf-8')}")
            elif response.status != 200:
                print(f"Error sending message to connection {connection_id}: Status {response.status}")
                print(f"Response: {response.data.decode('utf-8')}")
        except Exception as e:
            print(f"Error sending to connection {connection_id}: {str(e)}")

    def broadcast_message(self, data: Dict[str, Any]) -> None:
        print(f"Broadcasting message to all connections")
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

def create_websocket_service(endpoint: str, connections_table_name: str) -> Optional[WebSocketService]:
    """Create WebSocket service with error handling."""
    try:
        if not endpoint or not connections_table_name:
            print(f"Missing required parameters: endpoint={endpoint}, connections_table_name={connections_table_name}")
            return None
            
        return WebSocketService(endpoint, connections_table_name)
    except Exception as e:
        print(f"Error creating WebSocket service: {str(e)}")
        return None