import json
import boto3
import os
import logging
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")
bedrock_client = boto3.client("bedrock-runtime", region_name="us-west-2")

def invoke_bedrock(prompt, model_id="anthropic.claude-3-5-sonnet-20241022-v2:0", max_length=2048):
    """
    Invokes a Bedrock Claude model with a text prompt.
    Returns the model's textual response.
    """ 
    request_body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_length,
        "temperature": 0.1,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ]
    })

    try:
        logger.info("Invoking Bedrock model...")
        response = bedrock_client.invoke_model(
            modelId=model_id,
            body=request_body
        )
        model_response = json.loads(response["body"].read())

        # Attempt to parse the known structure from Claude
        if "content" in model_response and isinstance(model_response["content"], list):
            # Typically: model_response["content"][0]["text"]
            return model_response["content"][0]["text"].strip()

        # Fallback logic, if the structure is different:
        if "completion" in model_response:
            return model_response["completion"].strip()

        return json.dumps({"error": "No completion or content in response."})

    except (ClientError, Exception) as e:
        logger.error(f"Error invoking model: {e}")
        raise e

def build_prompt(clusters_data):
    """
    Builds a strict JSON prompt for cluster-level analysis.
    We ONLY want the final JSON with these keys:
      clusters -> [
         {
           clusterName,
           clusterDescription,
           overallSentiment,
           repOrg,
           recActions,
           relComments
         }
      ]
    No extra commentary or text.

    We also instruct that each cluster must appear, no merging or omission.
    """

    # Provide example JSON structure up front
    example_json_structure = """
{
  "clusters": [
    {
      "clusterName": "Worker Safety Standards",
      "clusterDescription": "Describes the main theme or focus of this cluster.",
      "overallSentiment": "Positive",
      "repOrg": ["Southern Poverty Law Center", "Farmworker Justice"],
      "recActions": ["Withdraw proposed rule", "Implement safety standards"],
      "relComments": ["comment1", "comment2", "comment3"]
    },
    {
      "clusterName": "Economic Impact",
      "clusterDescription": "Describes how new regulations might affect the economy.",
      "overallSentiment": "Neutral",
      "repOrg": ["American Farm Bureau Federation"],
      "recActions": ["Conduct economic impact assessment", "Delay implementation"],
      "relComments": ["comment1", "comment2", "comment3"]
    }
  ]
}
"""

    # Build a text snippet listing each cluster and sampled comments
    snippet = ""
    for cluster_info in clusters_data:
        cluster_id = cluster_info["cluster_name"]
        sample_comments = cluster_info["sample_comments"]
        snippet += f"Cluster: {cluster_id}\n"
        snippet += "Sample Comments:\n"
        for c in sample_comments:
            snippet += f" - {c}\n"
        snippet += "\n"

    num_clusters = len(clusters_data)

    # Final prompt to the model
    prompt = f"""
You are an LLM that produces STRICT JSON ONLY, nothing else.
Your output must match exactly this structure with ALL clusters included.

You have EXACTLY {num_clusters} clusters. You MUST produce the same number of cluster objects. 
You must NOT merge, combine, or omit any cluster. 
If the user has 9 clusters, output 9 clusters in the final JSON.

Here is the desired JSON format (an example):

{example_json_structure}

For each cluster, fill in:
- clusterName
- clusterDescription (a concise summary of the cluster)
- overallSentiment: choose "Positive", "Neutral", or "Negative"
- repOrg: list of representative organizations or stakeholders
- recActions: recommended actions or changes
- relComments: a short curated subset of sample comments from that cluster

Below is the data you have:

{snippet}

Return ONLY valid JSON in the final answer. 
IMPORTANT: You must produce an array of {num_clusters} cluster objects. Do not add extra commentary or text.
"""
    return prompt

def lambda_handler(event, context):
    """
    Triggered when a new clustered CSV is uploaded to 'after-clustering/' in S3.
    1. Download the CSV.
    2. Group by kmeans_cluster_id and sample up to 5 comments.
    3. Build a prompt for each cluster.
    4. Invoke Claude via Bedrock to get a strictly formatted JSON.
    5. Parse and store the JSON in 'analysis-json/' folder.
    """
    logger.info("Event received:")
    logger.info(json.dumps(event, indent=2))

    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        logger.info(f"New file in S3 => s3://{bucket_name}/{object_key}")

        # 1. Download the CSV locally
        local_csv_path = f"/tmp/{os.path.basename(object_key)}"
        s3_client.download_file(bucket_name, object_key, local_csv_path)

        # 2. Read CSV, check for required columns
        df = pd.read_csv(local_csv_path)
        if "kmeans_cluster_id" not in df.columns or "comment_text" not in df.columns:
            raise ValueError("CSV missing 'kmeans_cluster_id' or 'comment_text' columns.")

        # Group and sample
        cluster_data_list = []
        grouped = df.groupby("kmeans_cluster_id")
        for cluster_id, grp in grouped:
            sample_count = min(5, len(grp))
            sample_df = grp.sample(n=sample_count) if sample_count > 0 else grp
            sample_comments = sample_df["comment_text"].tolist()

            cluster_data_list.append({
                "cluster_name": f"Cluster_{cluster_id}",
                "sample_comments": sample_comments
            })

        # 3. Build the prompt
        prompt_text = build_prompt(cluster_data_list)

        # *** PRINT THE PROMPT TO LOGS FOR DEBUGGING ***
        logger.info("======== BEDROCK PROMPT START ========")
        logger.info(prompt_text)
        logger.info("======== BEDROCK PROMPT END   ========")

        # 4. Invoke Bedrock
        response_text = invoke_bedrock(prompt_text)
        logger.info("LLM response received.")

        # 5. Validate JSON
        try:
            final_json = json.loads(response_text)
        except json.JSONDecodeError:
            raise ValueError(f"LLM response is not valid JSON:\n{response_text}")

        # Make sure the LLM returned the same number of clusters we asked for
        returned_clusters = final_json.get("clusters", [])
        if len(returned_clusters) != len(cluster_data_list):
            logger.warning(
                f"WARNING: Expected {len(cluster_data_list)} clusters but got {len(returned_clusters)}."
                " The model might have merged or omitted clusters."
            )

        # Save JSON to 'analysis-json/' folder
        json_key = f"analysis-json/{os.path.splitext(os.path.basename(object_key))[0]}_analysis.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=json.dumps(final_json, indent=2),
            ContentType="application/json"
        )

        logger.info(f"Saved analysis JSON to s3://{bucket_name}/{json_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Cluster analysis completed successfully.",
                "analysisJsonUri": f"s3://{bucket_name}/{json_key}",
                "llm_raw_output": response_text  # optional to return raw output
            })
        }

    except Exception as e:
        logger.error(f"Error in cluster analysis: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }