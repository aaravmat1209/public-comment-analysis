from typing import Dict, Any, List
import math

def calculate_work_batches(
    total_comments: int,
    workers_per_batch: int = 4,
    page_size: int = 100
) -> Dict[str, Any]:
    """Calculate work ranges organized into batches of workers."""
    # Calculate total number of pages needed
    total_pages = math.ceil(total_comments / page_size)
    
    # Calculate number of batches needed
    total_batches = math.ceil(total_pages / workers_per_batch)
    
    batches = []
    current_worker_id = 0
    
    for batch_id in range(total_batches):
        batch_workers = []
        # Calculate how many workers needed for this batch
        workers_in_batch = min(
            workers_per_batch,
            total_pages - (batch_id * workers_per_batch)
        )
        
        for i in range(workers_in_batch):
            page_number = (batch_id * workers_per_batch) + i + 1  # API uses 1-based page numbers
            expected_comments = min(
                page_size,
                total_comments - ((page_number - 1) * page_size)
            )
            
            worker_range = {
                "batchId": batch_id,
                "workerId": current_worker_id,
                "pageNumber": page_number,
                "pageSize": page_size,
                "expectedComments": expected_comments
            }
            batch_workers.append(worker_range)
            current_worker_id += 1
        
        batches.append({
            "batchId": batch_id,
            "workers": batch_workers,
            "expectedWorkers": workers_in_batch
        })
    
    print(f"Created {len(batches)} batches for {total_comments} comments")
    for batch in batches:
        print(f"Batch {batch['batchId']}: {len(batch['workers'])} workers")
        for worker in batch['workers']:
            print(f"  Worker {worker['workerId']}: Page {worker['pageNumber']} (expecting {worker['expectedComments']} comments)")
    
    return {
        "batches": batches,
        "totalBatches": len(batches),
        "totalWorkers": current_worker_id,
        "workersPerBatch": workers_per_batch,
        "totalComments": total_comments,
        "pageSize": page_size
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Calculate work ranges organized into batches for sequential processing."""
    try:
        document_id = event['documentId']
        object_id = event['objectId']
        total_comments = event['totalComments']
        workers_per_batch = event.get('workersPerBatch', 4)
        
        print(f"Calculating batched work ranges for document {document_id} with {total_comments} comments")
        print(f"Using {workers_per_batch} workers per batch")
        
        # Calculate work ranges organized into batches
        work_batches = calculate_work_batches(
            total_comments,
            workers_per_batch=workers_per_batch
        )
        
        return {
            **work_batches,
            'documentId': document_id,
            'objectId': object_id,
            'currentBatch': 0  # Start with first batch
        }

    except Exception as e:
        print(f"Error calculating work batches: {str(e)}")
        raise