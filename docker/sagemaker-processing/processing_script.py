import argparse
import os
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sentence_transformers import SentenceTransformer
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def process_comments(input_file, output_file, n_clusters=10):
    logging.info("Reading input file...")
    df = pd.read_csv(input_file)

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
    logging.info("Saving results to output file...")
    df.to_csv("/opt/ml/processing/output/clustered_results.csv", index=False)
    logging.info(f"Results saved to {output_file}")

def main(input_data, output_data, object_name, n_clusters):
    # Input file path
    input_file = os.path.join(input_data, object_name)
    logging.info(f"Input file path: {input_file}")

    # Get document ID from input filename
    # Expected format: comments_DOCUMENT-ID_timestamp.csv
    doc_id = object_name.split('comments_')[1].split('_')[0]
    
    # Output file path - include document ID in output
    output_csv = os.path.join(output_data, f'clustered_results_{doc_id}.csv')
    logging.info(f"Output file path: {output_csv}")

    # Process the comments
    process_comments(input_file, output_csv, n_clusters=n_clusters)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process comments for clustering.")
    parser.add_argument('--input-data', type=str, required=True, help="Path to input data directory.")
    parser.add_argument('--output-data', type=str, required=True, help="Path to output data directory.")
    parser.add_argument('--object-name', type=str, required=True, help="Name of the input file to process.")
    parser.add_argument('--n-clusters', type=int, default=10, help="Number of clusters for KMeans.")
    args = parser.parse_args()

    main(args.input_data, args.output_data, args.object_name, args.n_clusters)