import json
import os
import boto3
import urllib3
import websocket
import threading
import time
from typing import Dict, Any, List, Optional

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Test the document processing backend"""
    try:
        # Configuration
        api_endpoint = os.environ['API_ENDPOINT'].rstrip('/')  # Remove trailing slash if present
        websocket_endpoint = os.environ['WEBSOCKET_ENDPOINT']
        
        # Test document IDs
        document_ids = ['FSIS-2024-0010-0001']
        
        # Dictionary to store progress updates
        progress_updates = {}
        final_analysis = {}
        
        # WebSocket message handler
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if data['type'] == 'PROGRESS_UPDATE':
                    doc_id = data['documentId']
                    progress_updates[doc_id] = {
                        'documentTitle': data.get('documentTitle', 'unknown'),
                        'status': data['status'],
                        'stage': data.get('stage', 'unknown'),
                        'progress': data.get('progress', 0),
                        'timestamp': data['timestamp']
                    }
                    print(f"Progress update for: {data}")
                    
                    # If progress is 100%, get the analysis results
                    if data.get('progress') == 100:
                        try:
                            print(f"Processing complete for {doc_id}, fetching analysis...")
                            http = urllib3.PoolManager()
                            response = http.request(
                                'GET',
                                f"{api_endpoint}/documents/{doc_id}",
                                headers={
                                    'Content-Type': 'application/json'
                                }
                            )
                            
                            if response.status == 200:
                                result = json.loads(response.data.decode('utf-8'))
                                final_analysis[doc_id] = result.get('analysis')
                                print(f"Analysis retrieved for {doc_id}")
                                print(json.dumps(final_analysis[doc_id], indent=2))
                            else:
                                print(f"Error getting analysis: {response.status}")
                                print(response.data.decode('utf-8'))
                                
                        except Exception as e:
                            print(f"Error fetching analysis: {str(e)}")
                    
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
        print(f"Submitting documents to {api_endpoint}/documents")
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
        
        print(f"POST Response Status: {response.status}")
        print(f"POST Response Body: {response.data.decode('utf-8')}")
        
        if response.status != 200:
            raise Exception(f"Failed to submit documents: {response.data.decode('utf-8')}")
        
        submission_result = json.loads(response.data.decode('utf-8'))
        print(f"Submission result: {json.dumps(submission_result, indent=2)}")
        
        # Monitor progress for up to 15 minutes
        start_time = time.time()
        timeout = 900  # 15 minutes
        check_interval = 10  # seconds between checks
        
        while time.time() - start_time < timeout:
            # Check if all documents are completed
            all_completed = True
            for doc_id in document_ids:
                if (doc_id not in progress_updates or 
                    progress_updates[doc_id]['progress'] < 100 or
                    doc_id not in final_analysis):
                    all_completed = False
                    break
            
            if all_completed:
                print("All documents processed successfully!")
                print("\nFinal Analysis Results:")
                for doc_id in document_ids:
                    print(f"\nDocument {doc_id}:")
                    print(json.dumps(final_analysis[doc_id], indent=2))
                break
            
            # Wait before checking again
            time.sleep(check_interval)
        else:
            print("Timeout waiting for processing to complete")
        
        # Close WebSocket connection
        ws.close()
        
        # Return final status
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Test completed',
                'submissionResult': submission_result,
                'finalProgress': progress_updates,
                'analysisResults': final_analysis
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