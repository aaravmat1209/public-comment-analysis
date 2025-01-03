from typing import Dict, Any

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Check if there are more batches to process.
    
    Args:
        event: Dict containing:
            - currentBatch: Current batch number (1-based)
            - totalBatches: Total number of batches
            
    Returns:
        Dict containing:
            - hasMoreBatches: Boolean indicating if more batches exist
    """
    try:
        current_batch = event.get('currentBatch', 0)
        total_batches = event.get('totalBatches', 0)
        
        print(f"Checking batch progress: {current_batch} of {total_batches}")
        
        return {
            'hasMoreBatches': current_batch + 1 < total_batches
        }
            
    except Exception as e:
        print(f"Error checking batch progress: {str(e)}")
        raise