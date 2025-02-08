#!/usr/bin/env python
# Install required packages
import subprocess
import sys

def install_packages():
    packages = [
        'pandas',
        'scikit-learn',
        'sentence-transformers',
        'PyPDF2',
        'beautifulsoup4',
        'nltk',
        'requests'
    ]
    for package in packages:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

print("Installing required packages...")
install_packages()

import argparse
import os
import json
import pandas as pd
import requests
import tempfile
import PyPDF2
import re
import nltk
from nltk.tokenize import sent_tokenize
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sentence_transformers import SentenceTransformer
import logging
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime, timezone

# Set up enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    logging.debug("Cleaning text content...")
    
    # Remove HTML tags if present
    text = BeautifulSoup(text, "html.parser").get_text()
    
    # Replace multiple newlines and spaces
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r' +', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?;:-]', ' ', text)
    
    # Normalize whitespace
    text = ' '.join(text.split())
    
    return text.strip()

def chunk_text(text: str, max_chunk_size: int = 500) -> List[str]:
    """Split text into meaningful chunks using sentence boundaries."""
    logging.debug(f"Chunking text with max size {max_chunk_size}")
    # First split into sentences
    sentences = sent_tokenize(text)
    
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        sentence = sentence.strip()
        sentence_length = len(sentence)
        
        if sentence_length > max_chunk_size:
            # If a single sentence is too long, split by words
            words = sentence.split()
            temp_chunk = []
            temp_length = 0
            
            for word in words:
                if temp_length + len(word) + 1 > max_chunk_size:
                    chunks.append(' '.join(temp_chunk))
                    temp_chunk = [word]
                    temp_length = len(word)
                else:
                    temp_chunk.append(word)
                    temp_length += len(word) + 1
                    
            if temp_chunk:
                chunks.append(' '.join(temp_chunk))
                
        elif current_length + sentence_length + 1 > max_chunk_size:
            # Start new chunk if adding this sentence would exceed limit
            chunks.append(' '.join(current_chunk))
            current_chunk = [sentence]
            current_length = sentence_length
        else:
            # Add sentence to current chunk
            current_chunk.append(sentence)
            current_length += sentence_length + 1
    
    # Add final chunk if any remains
    if current_chunk:
        chunks.append(' '.join(current_chunk))
    
    logging.debug(f"Created {len(chunks)} chunks")
    return chunks

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text content from a PDF file with enhanced error handling."""
    logging.info(f"Extracting text from PDF: {pdf_path}")
    try:
        text_parts = []
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)
            logging.info(f"PDF has {total_pages} pages")
            
            # Process each page
            for page_num, page in enumerate(pdf_reader.pages, 1):
                try:
                    # Extract text with error handling for each page
                    page_text = page.extract_text()
                    if page_text:
                        cleaned_text = clean_text(page_text)
                        text_parts.append(cleaned_text)
                        logging.debug(f"Successfully extracted text from page {page_num}")
                    else:
                        logging.warning(f"No text content found in page {page_num}")
                except Exception as e:
                    logging.warning(f"Error extracting text from PDF page {page_num}: {str(e)}")
                    continue
        
        combined_text = '\n'.join(text_parts)
        logging.info(f"Successfully extracted {len(text_parts)} pages of text")
        return combined_text
        
    except Exception as e:
        logging.error(f"Error processing PDF file: {str(e)}")
        return ""

def process_attachment(file_url: str, file_format: str) -> Optional[List[str]]:
    """Download and process an attachment, returning chunked text content."""
    logging.info(f"Processing attachment with format {file_format} from URL: {file_url}")
    try:
        # Download the file
        logging.debug("Downloading file...")
        response = requests.get(file_url, timeout=30)
        response.raise_for_status()
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(response.content)
            temp_path = temp_file.name
            logging.debug(f"Saved to temporary file: {temp_path}")
            
        try:
            text_content = ""
            if file_format.lower() == 'pdf':
                text_content = extract_text_from_pdf(temp_path)
            elif file_format.lower() in ['txt', 'text']:
                with open(temp_path, 'r', encoding='utf-8', errors='replace') as f:
                    text_content = clean_text(f.read())
                    logging.debug("Successfully read text file")
            
            if text_content:
                # Chunk the cleaned text
                chunks = chunk_text(text_content)
                logging.info(f"Successfully processed attachment into {len(chunks)} chunks")
                return chunks
            else:
                logging.warning("No text content extracted from attachment")
            return None
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_path):
                os.unlink(temp_path)
                logging.debug("Cleaned up temporary file")
                
    except Exception as e:
        logging.error(f"Error processing attachment: {str(e)}", exc_info=True)
        return None

def combine_comments_and_attachments(comments_df: pd.DataFrame, attachments_df: pd.DataFrame) -> pd.DataFrame:
    """Combine comment text with its attachment texts."""
    logging.info("Combining comments with their attachments...")
    
    # Create a dictionary to store attachment texts by comment_id
    attachment_texts = {}
    
    # Process attachments if they exist and aren't empty
    if not attachments_df.empty:
        for _, row in attachments_df.iterrows():
            comment_id = row['comment_id']
            # Only process if file_url exists and isn't empty
            if pd.notna(row.get('file_url')) and row['file_url'].strip():
                chunks = process_attachment(row['file_url'], row['file_format'])
                if chunks:
                    if comment_id not in attachment_texts:
                        attachment_texts[comment_id] = []
                    attachment_texts[comment_id].extend(chunks)
    
    # Combine comments with their attachments
    combined_texts = []
    for _, comment in comments_df.iterrows():
        comment_id = comment['comment_id']
        # Get comment text
        combined_text = comment['comment_text']
        
        # Add attachment texts if any exist
        if comment_id in attachment_texts:
            attachment_text = ' '.join(attachment_texts[comment_id])
            combined_text = f"{combined_text}\n\n{attachment_text}"
        
        combined_texts.append({
            'comment_id': comment_id,
            'comment_text': comment['comment_text'],
            'combined_text': combined_text,
            'has_attachments': comment_id in attachment_texts,
            'posted_date': comment.get('posted_date', '')  # Ensure posted_date is included
        })
    
    result_df = pd.DataFrame(combined_texts)
    logging.info(f"Created {len(result_df)} combined documents, {len(attachment_texts)} with attachments")
    return result_df

def deduplicate_comments(df):
    """
    Remove comments with exactly identical text (case insensitive),
    preserving comments that say 'See Attached' since they may have different attachments.
    Also tracks duplicate information for reporting.
    
    Args:
        df (pandas.DataFrame): DataFrame containing comments with 'comment_text' column
        
    Returns:
        tuple: (deduplicated DataFrame, statistics dict, duplicate groups dict)
    """
    import pandas as pd
    print(f"Starting exact text deduplication of {len(df)} comments...")
    
    # Convert text to lowercase for case-insensitive comparison
    df['text_lower'] = df['comment_text'].str.lower()
    
    # Find comments that are just "see attached" (case insensitive)
    see_attached_mask = df['text_lower'].str.strip().isin(['see attached', 'see attachment', 'see attachments'])
    
    # Split into "see attached" and regular comments
    see_attached_df = df[see_attached_mask].copy()
    regular_df = df[~see_attached_mask].copy()
    
    # Count occurrences of each unique comment text
    text_counts = regular_df['text_lower'].value_counts()
    duplicate_texts = text_counts[text_counts > 1].index
    
    # For regular comments, keep only the first occurrence of each text
    deduplicated_regular = regular_df.drop_duplicates(subset='text_lower', keep='first')
    
    # Combine back with "see attached" comments
    final_df = pd.concat([deduplicated_regular, see_attached_df])
    
    # Create duplicate groups mapping
    duplicate_groups = {}
    for text in duplicate_texts:
        # Get all comments with this text
        duplicate_indices = regular_df[regular_df['text_lower'] == text].index.tolist()
        duplicate_comments = regular_df.loc[duplicate_indices][['comment_id', 'comment_text', 'posted_date']].to_dict('records')
        
        # Use the first comment's ID as the group identifier
        group_id = str(duplicate_comments[0]['comment_id'])
        duplicate_groups[group_id] = {
            'comment_text': duplicate_comments[0]['comment_text'],
            'duplicate_count': len(duplicate_comments),
            'duplicate_comments': duplicate_comments
        }

    # Calculate statistics
    stats = {
        'total_comments': len(df),
        'duplicate_comments_removed': len(regular_df) - len(deduplicated_regular),
        'see_attached_comments': len(see_attached_df),
        'remaining_comments': len(final_df),
        'duplicate_groups': len(duplicate_groups)
    }
    
    print(f"Deduplication stats: {stats}")
    
    # Clean up temporary column
    final_df = final_df.drop('text_lower', axis=1)
    
    return final_df, stats, duplicate_groups

def cluster_text(texts: List[str], n_clusters: int = 10) -> Tuple[List[int], float]:
    """Cluster text content using sentence embeddings and KMeans."""
    logging.info(f"Starting text clustering for {len(texts)} items with {n_clusters} clusters")
    
    # Load pre-trained model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Generate embeddings with batching for long texts
    logging.info("Generating text embeddings...")
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        normalize_embeddings=True
    )
    
    # Adjust number of clusters if necessary
    n_clusters = min(n_clusters, len(texts))
    logging.info(f"Using {n_clusters} clusters for {len(texts)} items")
    
    # Apply KMeans clustering
    kmeans = KMeans(
        n_clusters=n_clusters,
        random_state=42,
        n_init=10
    )
    clusters = kmeans.fit_predict(embeddings)
    
    # Calculate silhouette score if more than one cluster
    if len(set(clusters)) > 1:
        silhouette = silhouette_score(embeddings, clusters)
        logging.info(f"Calculated silhouette score: {silhouette}")
    else:
        silhouette = 0.0
        logging.warning("Only one cluster found, silhouette score set to 0")
    
    return clusters, silhouette

def numpy_json_converter(obj):
    """Convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
        np.int16, np.int32, np.int64, np.uint8,
        np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def process_content(input_file: str, output_file: str, metadata_file: str, attachments_file: str = None, n_clusters: int = 10):
    """Process comments and attachments for clustering."""
    logging.info("Reading input files...")
    
    # Read comments
    comments_df = pd.read_csv(input_file)
    initial_count = len(comments_df)
    logging.info(f"Read {initial_count} comments")
    
    # Read attachments if available
    attachments_df = pd.DataFrame()
    if attachments_file and os.path.exists(attachments_file):
        attachments_df = pd.read_csv(attachments_file)
        logging.info(f"Read {len(attachments_df)} attachments")
    else:
        logging.warning("Attachments file not found or couldn't be read")
    
    # Initialize results dictionary
    results = {
        'processing_metadata': {
            'total_comments': initial_count,
            'comments_with_attachments': 0,
            'total_attachments': len(attachments_df) if not attachments_df.empty else 0,
            'processing_timestamp': datetime.now(timezone.utc).isoformat()
        }
    }
    
    # Combine comments with their attachments
    combined_df = combine_comments_and_attachments(comments_df, attachments_df)
    results['processing_metadata']['comments_with_attachments'] = combined_df['has_attachments'].sum()
    
    # Deduplicate comments before clustering
    if len(combined_df) > 0:
        combined_df, dedup_stats, duplicate_groups = deduplicate_comments(combined_df)
        results['deduplication_metadata'] = dedup_stats
        results['duplicate_groups'] = duplicate_groups
        
        # Cluster the deduplicated comments
        clusters, silhouette = cluster_text(combined_df['combined_text'].tolist(), n_clusters)
        combined_df['cluster_id'] = clusters
        
        results['clustering_metadata'] = {
            'n_clusters': len(set(clusters)),
            'silhouette_score': float(silhouette),
            'comments_per_cluster': combined_df.groupby('cluster_id').size().to_dict()
        }
        
        # Create the combined output
        combined_output = {
            'metadata': results,
            'clustered_data': combined_df.to_dict(orient='records')
        }
        
        # Save combined output
        with open(output_file, 'w') as f:
            json.dump(combined_output, f, indent=2, default=numpy_json_converter)
            
        logging.info(f"Saved combined clustering results and metadata to {output_file}")
    
        return combined_output
    else:
        logging.error("No data to process")
        return None

def find_input_files(input_data: str, doc_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Find comments and attachments files in the document directory."""
    doc_dir = os.path.join(input_data, doc_id)
    logging.info(f"Looking for files in directory: {doc_dir}")
    
    try:
        # List all files in the directory
        files = os.listdir(doc_dir)
        logging.info(f"Files in directory {doc_dir}:")
        for file in files:
            logging.info(f"  - {file}")
            
        # Look for comments and attachments files
        comments_file = None
        attachments_file = None
        
        for file in files:
            if file.startswith('comments_') and file.endswith('.csv'):
                comments_file = os.path.join(doc_dir, file)
            elif file.startswith('attachments_') and file.endswith('.csv'):
                attachments_file = os.path.join(doc_dir, file)
        
        if comments_file:
            logging.info(f"Found comments file: {comments_file}")
        else:
            logging.warning("Comments file not found")
            
        if attachments_file:
            logging.info(f"Found attachments file: {attachments_file}")
        else:
            logging.warning("Attachments file not found")
            
        return comments_file, attachments_file
        
    except Exception as e:
        logging.error(f"Error finding input files: {str(e)}")
        return None, None

def main(input_data, output_data, doc_id, n_clusters):
    """Process input files and save clustered results."""
    logging.info(f"Starting processing for document ID: {doc_id}")
    
    # Find input files
    comments_file, attachments_file = find_input_files(input_data, doc_id)
    
    if not comments_file:
        raise ValueError(f"Comments file not found for document {doc_id}")
    
    # Create output paths - now using JSON for combined output
    output_filename = f'clustered_results_{doc_id}.json'
    output_path = os.path.join(output_data, output_filename)
    
    logging.info(f"Output will be written to: {output_path}")
    
    # Process content and save combined output
    combined_output = process_content(
        comments_file,
        output_path,
        None,  # metadata file no longer needed as it's combined
        attachments_file,
        n_clusters=n_clusters
    )
    
    if combined_output:
        logging.info("Processing completed successfully")
        return combined_output
    else:
        raise ValueError("Processing failed - no output generated")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process comments and attachments for clustering.")
    parser.add_argument('--input-data', type=str, required=True, help="Path to input data directory.")
    parser.add_argument('--output-data', type=str, required=True, help="Path to output data directory.")
    parser.add_argument('--doc-id', type=str, required=True, help="Document ID.")
    parser.add_argument('--n-clusters', type=int, default=10, help="Number of clusters for KMeans.")
    args = parser.parse_args()

    main(args.input_data, args.output_data, args.doc_id, args.n_clusters)