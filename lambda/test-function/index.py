import json
import os
import boto3
import urllib3
import websocket
import threading
import time
from typing import Dict, Any, List

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Test the document processing backend"""
    try:
        # Configuration
        api_endpoint = os.environ['API_ENDPOINT']
        websocket_endpoint = os.environ['WEBSOCKET_ENDPOINT']
        
        # Test document IDs
        document_ids = ['EPA-R10-OW-2017-0369-0001']
        
        # Dictionary to store progress updates
        progress_updates = {}
        
        # WebSocket message handler
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data['type'] == 'PROGRESS_UPDATE':
                    doc_id = data['documentId']
                    progress_updates[doc_id] = {
                        'status': data['status'],
                        'progress': data.get('progress', 0),
                        'timestamp': data['timestamp']
                    }
                    print(f"Progress update for {doc_id}: {progress_updates[doc_id]}")
            except Exception as e:
                print(f"Error processing message: {str(e)}")
                print(f"Message content: {message}")
        
        # WebSocket error handler
        def on_error(ws, error):
            print(f"WebSocket error: {error}")
        
        # WebSocket close handler
        def on_close(ws, close_status_code, close_msg):
            print("WebSocket connection closed")
        
        # WebSocket connection handler
        def on_open(ws):
            print("WebSocket connection established")
        
        # Create WebSocket connection
        ws = websocket.WebSocketApp(
            websocket_endpoint,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open
        )
        
        # Start WebSocket connection in a separate thread
        ws_thread = threading.Thread(target=ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        
        # Initialize HTTP client
        http = urllib3.PoolManager()
        
        # Submit documents for processing
        response = http.request(
            'POST',
            f"{api_endpoint}/documents",
            headers={
                'Content-Type': 'application/json'
            },
            body=json.dumps({
                'documentIds': document_ids
            }).encode('utf-8')
        )
        
        print(f"RESPONSE FROM POST: {response}")
        
        if response.status != 200:
            raise Exception(f"Failed to submit documents: {response.data.decode('utf-8')}")
        
        submission_result = json.loads(response.data.decode('utf-8'))
        print(f"Submission result: {json.dumps(submission_result, indent=2)}")
        
        # Monitor progress for 5 minutes
        start_time = time.time()
        timeout = 300  # 5 minutes
        
        while time.time() - start_time < timeout:
            # Check if all documents are completed
            all_completed = True
            for doc_id in document_ids:
                if doc_id not in progress_updates or progress_updates[doc_id]['progress'] < 100:
                    all_completed = False
                    break
            
            if all_completed:
                print("All documents processed successfully!")
                break
            
            # Wait a bit before checking again
            time.sleep(5)
        
        # Close WebSocket connection
        ws.close()
        
        # Return final status
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Test completed',
                'submissionResult': submission_result,
                'finalProgress': progress_updates
            }, indent=2)
        }
        
    except Exception as e:
        print(f"Error in test: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }