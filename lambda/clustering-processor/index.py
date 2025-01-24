import json
import boto3
import os
import uuid
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to start a SageMaker processing job when a new file is uploaded to S3.
    """
    # Initialize SageMaker client
    sagemaker_client = boto3.client('sagemaker')

    # Get environment variables
    IMAGE_URI = os.environ.get('IMAGE_URI', '904233123149.dkr.ecr.us-west-2.amazonaws.com/sagemaker-processing-image:latest')  # Docker image URI
    ROLE_ARN = os.environ.get('ROLE_ARN','arn:aws:iam::904233123149:role/sagemaker-processing')    # SageMaker execution role ARN
    REGION = os.environ.get('AWS_REGION', 'us-west-2')  # Default region

    if not IMAGE_URI or not ROLE_ARN:
        logger.error("Environment variables IMAGE_URI and ROLE_ARN must be set.")
        return {
            'statusCode': 500,
            'body': json.dumps('Missing IMAGE_URI or ROLE_ARN environment variables.')
        }

    try:
        # Parse S3 event
        records = event.get('Records', [])
        if not records:
            logger.error("No records found in the event.")
            return {
                'statusCode': 400,
                'body': json.dumps('No records found in the event.')
            }

        for record in records:
            # Get the S3 bucket and object key from the event
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')

            if not bucket or not key:
                logger.error("Bucket or key not found in the event.")
                continue

            logger.info(f"New file detected: s3://{bucket}/{key}")

            # Construct input and output S3 paths
            input_s3_uri = f"s3://{bucket}/{key}"
            
            # Determine the output key by replacing 'before-clustering/' with 'after-clustering/'
            if key.startswith('before-clustering/'):
                output_key_prefix = key.replace('before-clustering/', 'after-clustering/')
            else:
                # If not starting with 'before-clustering/', place it in after-clustering/
                output_key_prefix = f"after-clustering/"

            output_s3_uri = f"s3://{bucket}/after-clustering/"
            

            # Generate a unique job name
            job_name = f"processing-job-{uuid.uuid4()}"

            logger.info(f"Starting SageMaker processing job: {job_name}")
            logger.info(f"Input S3 URI: {input_s3_uri}")
            logger.info(f"Output S3 URI: {output_s3_uri}")

            # Define the processing job parameters
            response = sagemaker_client.create_processing_job(
                ProcessingJobName=job_name,
                AppSpecification={
                    'ImageUri': IMAGE_URI,
                    'ContainerEntrypoint': [
                        "python3",
                        "/opt/ml/processing/input/code/processing_script.py"
                    ],
                    'ContainerArguments': [
                        "--input-data", "/opt/ml/processing/input/data",
                        "--output-data", "/opt/ml/processing/output",
                        "--object-name", os.path.basename(key),
                        "--n-clusters", "10"  # Adjust the number of clusters as needed
                    ]
                },
                RoleArn=ROLE_ARN,
                ProcessingInputs=[
                    {
                        'InputName': 'input-data',
                        'S3Input': {
                            'S3Uri': input_s3_uri,
                            'LocalPath': '/opt/ml/processing/input/data',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                        }
                    },
                    {
                        'InputName': 'code',
                        'S3Input': {
                            'S3Uri': f"s3://{bucket}/process/processing_script.py",
                            'LocalPath': '/opt/ml/processing/input/code',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                        }
                    }
                ],
                ProcessingOutputConfig={
                    'Outputs': [
                        {
                            'OutputName': 'output-data',
                            'S3Output': {
                                'S3Uri': output_s3_uri,
                                'LocalPath': '/opt/ml/processing/output',
                                'S3UploadMode': 'EndOfJob',
                            }
                        },
                    ]
                },
                ProcessingResources={
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': 'ml.c5.xlarge',  # Adjust based on your needs
                        'VolumeSizeInGB': 10,          # Adjust based on your data size
                    }
                },
                StoppingCondition={
                    'MaxRuntimeInSeconds': 3600  # Adjust as needed
                }
            )

            logger.info(f"SageMaker processing job {job_name} started.")

        return {
            'statusCode': 200,
            'body': json.dumps('SageMaker processing job started successfully.')
        }

    except Exception as e:
        logger.error(f"Error starting SageMaker processing job: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error starting SageMaker processing job: {str(e)}")
        }