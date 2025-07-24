# batch-checker lambda

from typing import Dict, Any, List

def check_for_incomplete_items(batch_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Check if any batch items were only partially completed."""
    if not batch_results:
        return []
    
    incomplete_items = []
    for result in batch_results:
        payload = result.get('Payload', {})
        if payload.get('isComplete') is False or payload.get('needsReprocessing') is True:
            incomplete_items.append(payload)
    
    return incomplete_items

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Check if there are more batches to process or if current batch needs reprocessing.
    
    Args:
        event: Dict containing:
            - currentBatch: Current batch number (0-based)
            - totalBatches: Total number of batches
            - batchResults: Results from the current batch processing
            
    Returns:
        Dict containing:
            - hasMoreBatches: Boolean indicating if more batches exist
            - needsReprocessing: Boolean indicating if current batch needs reprocessing
            - incompleteItems: List of incomplete items that need reprocessing
    """
    try:
        current_batch = event.get('currentBatch', 0)
        total_batches = event.get('totalBatches', 0)
        batch_results = event.get('batchResults', [])
        
        print(f"Checking batch progress: {current_batch + 1} of {total_batches}")
        
        # Check if any items in the current batch were incomplete
        incomplete_items = check_for_incomplete_items(batch_results)
        needs_reprocessing = len(incomplete_items) > 0
        
        if needs_reprocessing:
            print(f"Found {len(incomplete_items)} incomplete items that need reprocessing")
            return {
                'hasMoreBatches': True,  # Continue processing
                'needsReprocessing': True,
                'incompleteItems': incomplete_items,
                'currentBatch': current_batch  # Stay on current batch
            }
        
        # Check if there are more batches
        has_more_batches = current_batch + 1 < total_batches
        print(f"Has more batches: {has_more_batches}")
        
        return {
            'hasMoreBatches': has_more_batches,
            'needsReprocessing': False
        }
            
    except Exception as e:
        print(f"Error checking batch progress: {str(e)}")
        raise