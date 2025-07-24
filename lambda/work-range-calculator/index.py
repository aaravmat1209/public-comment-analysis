from typing import Dict, Any, List
import math
import os
import boto3
import json
from datetime import datetime, timezone

def get_checkpoint(document_id, worker_id, page_number):
    """Get the last checkpoint for a specific work range."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['STATE_TABLE_NAME'])
    
    checkpoint_id = f"checkpoint_{worker_id}_{page_number}"
    
    try:
        response = table.get_item(
            Key={
                'documentId': document_id,
                'chunkId': checkpoint_id
            }
        )
        
        if 'Item' in response:
            return json.loads(response['Item']['checkpoint'])
        return None
    except Exception as e:
        print(f"Error retrieving checkpoint: {str(e)}")
        return None

def calculate_work_batches(
    document_id: str,
    total_comments: int,
    workers_per_batch: int = 2,
    max_page_size: int = 250,
    reprocess_items: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Calculate work ranges organized into batches for all sets."""
    # If we have items to reprocess, create a special batch for them
    if reprocess_items and len(reprocess_items) > 0:
        print(f"Creating reprocessing batch for {len(reprocess_items)} items")
        
        # Create a batch with the incomplete items
        reprocess_workers = []
        for item in reprocess_items:
            worker_id = item.get('workerId')
            page_number = item.get('pageNumber')
            
            # Get the original work range for this item
            worker_range = {
                "batchId": 0,  # Special batch for reprocessing
                "workerId": worker_id,
                "pageNumber": page_number,
                "pageSize": max_page_size,
                "expectedComments": max_page_size,  # We don't know exactly how many
                "setNumber": 1,  # Always set 1 for reprocessing
                "isReprocessing": True
            }
            reprocess_workers.append(worker_range)
        
        # Create a single batch for all reprocessing items
        batches = [{
            "batchId": 0,
            "workers": reprocess_workers,
            "expectedWorkers": len(reprocess_workers),
            "setNumber": 1,
            "isReprocessing": True
        }]
        
        return {
            "batches": batches,
            "totalBatches": 1,  # Just one batch for reprocessing
            "totalWorkers": len(reprocess_workers),
            "workersPerBatch": len(reprocess_workers),
            "totalComments": total_comments,
            "pageSize": max_page_size,
            "expectedSets": 1,
            "commentsPerSet": total_comments,
            "isReprocessing": True
        }
    
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
                    # Check if this page has a checkpoint
                    checkpoint = get_checkpoint(document_id, current_worker_id, page_number)
                    
                    # Skip pages that are already fully processed
                    if checkpoint and checkpoint.get('completed', False):
                        print(f"Skipping worker {current_worker_id}, page {page_number} - already completed")
                        current_worker_id += 1
                        continue
                    
                    worker_range = {
                        "batchId": global_batch_id,  # Use global batch ID for workers
                        "workerId": current_worker_id,
                        "pageNumber": page_number,
                        "pageSize": max_page_size,
                        "expectedComments": expected_comments,
                        "setNumber": current_set
                    }
                    
                    # Add checkpoint information if available
                    if checkpoint:
                        worker_range["hasCheckpoint"] = True
                        worker_range["commentOffset"] = checkpoint.get('comment_offset', 0)
                    
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
    
def calculate_workers_per_batch(apiRateLimit: str) -> int:
    print(f"API Rate Limit set to: {apiRateLimit}")
    # Reduce workers per batch to avoid timeouts
    workers_per_batch = min(4, math.floor((int(apiRateLimit)-50) / 250))
    print(f"Max workers per batch: {workers_per_batch}")
    return workers_per_batch

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Calculate work ranges for all sets of comments."""
    try:
        document_id = event['documentId']
        object_id = event['objectId']
        total_comments = event['totalComments']
        workers_per_batch = calculate_workers_per_batch(os.environ['API_RATE_LIMIT'])
        current_set = event.get('currentSet', 1)
        last_modified_date = event.get('lastModifiedDate')
        
        # Check if we need to reprocess any items
        reprocess_items = None
        if 'batchCheck' in event and event['batchCheck'].get('Payload', {}).get('needsReprocessing'):
            reprocess_items = event['batchCheck']['Payload'].get('incompleteItems', [])
            print(f"Need to reprocess {len(reprocess_items)} items")
        
        print(f"Calculating work ranges for document {document_id}")
        print(f"Total comments: {total_comments}, Workers per batch: {workers_per_batch}")
        print(f"Current set: {current_set}, Last modified date: {last_modified_date}")
        
        # Calculate work ranges for all sets
        work_batches = calculate_work_batches(
            document_id,
            total_comments,
            workers_per_batch=workers_per_batch,
            max_page_size=100,  # Reduce page size to avoid timeouts
            reprocess_items=reprocess_items
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