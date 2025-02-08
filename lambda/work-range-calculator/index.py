from typing import Dict, Any, List
import math

def calculate_work_batches(
    total_comments: int,
    workers_per_batch: int = 2,
    max_page_size: int = 250
) -> Dict[str, Any]:
    """Calculate work ranges organized into batches for all sets."""
    # Calculate parameters within API limits
    max_pages_per_set = 20
    comments_per_set = max_pages_per_set * max_page_size  # 5000 comments per set
    total_sets = math.ceil(total_comments / comments_per_set)
    
    print(f"Calculating batches for {total_comments} comments")
    print(f"Comments per set: {comments_per_set}, Total sets needed: {total_sets}")
    
    batches = []
    current_worker_id = 0
    global_batch_id = 0  # Track global batch ID across all sets
    
    # Calculate batches for all sets
    for current_set in range(1, total_sets + 1):
        remaining_comments = min(
            comments_per_set,
            total_comments - ((current_set - 1) * comments_per_set)
        )
        pages_needed = math.ceil(remaining_comments / max_page_size)
        
        print(f"Set {current_set}: Need {pages_needed} pages for {remaining_comments} comments")
        
        # Calculate batches for this set
        total_batches_for_set = math.ceil(pages_needed / workers_per_batch)
        
        for local_batch_id in range(total_batches_for_set):
            batch_workers = []
            base_page = (local_batch_id * workers_per_batch) + 1
            
            # Calculate how many workers needed for this batch
            remaining_pages = pages_needed - (local_batch_id * workers_per_batch)
            workers_in_batch = min(workers_per_batch, remaining_pages)
            
            for i in range(workers_in_batch):
                page_number = base_page + i
                expected_comments = min(
                    max_page_size,
                    remaining_comments - ((page_number - 1) * max_page_size)
                )
                
                if expected_comments > 0:
                    worker_range = {
                        "batchId": global_batch_id,  # Use global batch ID for workers
                        "workerId": current_worker_id,
                        "pageNumber": page_number,
                        "pageSize": max_page_size,
                        "expectedComments": expected_comments,
                        "setNumber": current_set
                    }
                    batch_workers.append(worker_range)
                    current_worker_id += 1
            
            if batch_workers:  # Only add batch if it has workers
                batches.append({
                    "batchId": global_batch_id,  # Use same global batch ID for batch
                    "workers": batch_workers,
                    "expectedWorkers": len(batch_workers),
                    "setNumber": current_set
                })
                global_batch_id += 1  # Increment global batch ID after adding batch
    
    print(f"Created {len(batches)} total batches across {total_sets} sets")
    for batch in batches:
        print(f"Batch {batch['batchId']} (Set {batch['setNumber']}): {len(batch['workers'])} workers")
        for worker in batch['workers']:
            print(f"  Worker {worker['workerId']}: Page {worker['pageNumber']} (expecting {worker['expectedComments']} comments)")
    
    return {
        "batches": batches,
        "totalBatches": len(batches),
        "totalWorkers": current_worker_id,
        "workersPerBatch": workers_per_batch,
        "totalComments": total_comments,
        "pageSize": max_page_size,
        "expectedSets": total_sets,
        "commentsPerSet": comments_per_set
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Calculate work ranges for all sets of comments."""
    try:
        document_id = event['documentId']
        object_id = event['objectId']
        total_comments = event['totalComments']
        workers_per_batch = event.get('workersPerBatch', 2)
        current_set = event.get('currentSet', 1)
        last_modified_date = event.get('lastModifiedDate')
        
        print(f"Calculating work ranges for document {document_id}")
        print(f"Total comments: {total_comments}, Workers per batch: {workers_per_batch}")
        print(f"Current set: {current_set}, Last modified date: {last_modified_date}")
        
        # Calculate work ranges for all sets
        work_batches = calculate_work_batches(
            total_comments,
            workers_per_batch=workers_per_batch,
            max_page_size=250
        )
        
        return {
            **work_batches,
            'documentId': document_id,
            'objectId': object_id,
            'currentBatch': 0,  # Always start at first batch
            'lastModifiedDate': last_modified_date
        }

    except Exception as e:
        print(f"Error calculating work batches: {str(e)}")
        raise