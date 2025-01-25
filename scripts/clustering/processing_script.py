import argparse
import os
import json
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sentence_transformers import SentenceTransformer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def clean_comments(df):
    """Clean comments by removing unwanted entries and tracking duplicates."""
    logging.info("Starting comment cleaning process...")
    initial_count = len(df)
    
    # Convert comments to string type and lowercase for consistent comparison
    df['comment_text'] = df['comment_text'].astype(str).str.lower().str.strip()
    
    # Track "see attached" comments
    see_attached_patterns = [
        'see attached',
        'see attachment',
        'please see attached',
        'please see attachment'
    ]
    mask = ~df['comment_text'].str.contains('|'.join(see_attached_patterns), case=False)
    see_attached_comments = df[~mask].copy()
    df = df[mask]
    
    # Track duplicate comments
    duplicate_groups = []
    for _, group in df.groupby('comment_text'):
        if len(group) > 1:
            duplicate_groups.append({
                'comment_text': group['comment_text'].iloc[0],
                'count': len(group),
                'comment_ids': group['comment_id'].tolist()
            })
    
    # Remove exact duplicates based on comment text, keeping first occurrence
    df = df.drop_duplicates(subset=['comment_text'])
    
    final_count = len(df)
    removed_count = initial_count - final_count
    
    # Create metadata about cleaning
    cleaning_metadata = {
        'initial_count': initial_count,
        'final_count': final_count,
        'removed_count': removed_count,
        'see_attached_count': len(see_attached_comments),
        'see_attached_comments': see_attached_comments[['comment_id', 'comment_text']].to_dict('records') if 'comment_id' in see_attached_comments.columns else [],
        'duplicate_groups': duplicate_groups
    }
    
    logging.info(f"Removed {removed_count} comments:")
    logging.info(f" - Initial count: {initial_count}")
    logging.info(f" - Final count: {final_count}")
    logging.info(f" - See attached comments: {len(see_attached_comments)}")
    logging.info(f" - Duplicate groups: {len(duplicate_groups)}")
    
    return df, cleaning_metadata

def process_comments(input_file, output_file, metadata_file, n_clusters=10):
    """Process comments including cleaning, clustering, and metadata tracking."""
    logging.info("Reading input file...")
    df = pd.read_csv(input_file)

    # Clean comments before clustering
    logging.info("Cleaning comments...")
    df, cleaning_metadata = clean_comments(df)
    
    # Save cleaning metadata
    logging.info(f"Saving cleaning metadata to {metadata_file}")
    os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
    with open(metadata_file, 'w') as f:
        json.dump(cleaning_metadata, f, indent=2)
    
    if len(df) == 0:
        logging.error("No valid comments remaining after cleaning!")
        raise ValueError("No valid comments to cluster")

    # Extract comments
    logging.info("Extracting comments...")
    comments = df['comment_text'].astype(str).tolist()

    # Load pre-trained model for embeddings
    logging.info("Loading the SentenceTransformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Compute embeddings
    logging.info("Generating embeddings...")
    embeddings = model.encode(comments, show_progress_bar=True)

    # Normalize embeddings
    logging.info("Normalizing embeddings...")
    scaler = StandardScaler()
    embeddings_scaled = scaler.fit_transform(embeddings)

    # Adjust number of clusters if necessary
    n_clusters = min(n_clusters, len(df))
    logging.info(f"Using {n_clusters} clusters for {len(df)} comments")

    # Apply KMeans clustering
    logging.info("Applying KMeans clustering...")
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(embeddings_scaled)
    df['kmeans_cluster_id'] = clusters

    # Compute silhouette score
    logging.info("Calculating silhouette score...")
    if len(set(clusters)) > 1:
        silhouette_avg = silhouette_score(embeddings_scaled, clusters)
        logging.info(f"Silhouette Score for KMeans: {silhouette_avg}")
    else:
        logging.info("Only one cluster found. Silhouette score not applicable.")

    # Save results to output file
    logging.info(f"Saving results to output file: {output_file}")
    df.to_csv(output_file, index=False)
    logging.info(f"Results saved to {output_file}")

def main(input_data, output_data, object_name, n_clusters):
    """Process input comments file and save clustered results."""
    # Input file path
    input_file = os.path.join(input_data, object_name)
    logging.info(f"Processing input file: {input_file}")

    # Extract document ID from input filename
    try:
        if 'comments_' in object_name:
            doc_id = object_name.split('comments_')[1].split('_')[0]
        else:
            doc_id = os.path.dirname(input_file).split('/')[-2]
        logging.info(f"Extracted document ID: {doc_id}")
    except Exception as e:
        logging.error(f"Error extracting document ID from {object_name}: {str(e)}")
        raise

    # Create output paths
    output_filename = f'clustered_results_{doc_id}.csv'
    metadata_filename = f'metadata_{doc_id}.json'
    
    output_path = os.path.join(output_data, output_filename)
    metadata_path = os.path.join(output_data, metadata_filename)
    
    logging.info(f"Will save clustered results to: {output_path}")
    logging.info(f"Will save metadata to: {metadata_path}")

    # Process the comments
    process_comments(input_file, output_path, metadata_path, n_clusters=n_clusters)
    logging.info(f"Successfully saved clustered results as {output_filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process comments for clustering.")
    parser.add_argument('--input-data', type=str, required=True, help="Path to input data directory.")
    parser.add_argument('--output-data', type=str, required=True, help="Path to output data directory.")
    parser.add_argument('--object-name', type=str, required=True, help="Name of the input file to process.")
    parser.add_argument('--n-clusters', type=int, default=10, help="Number of clusters for KMeans.")
    args = parser.parse_args()

    main(args.input_data, args.output_data, args.object_name, args.n_clusters)