import * as cdk from 'aws-cdk-lib';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';
import * as path from 'path';
import { Construct } from 'constructs';

export class ECRStack extends cdk.Stack {
  public readonly imageUri: string;
  
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create ECR repository
    const repository = new ecr.Repository(this, 'SageMakerProcessingRepo', {
      repositoryName: 'sagemaker-processing-image',
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          maxImageCount: 3,
          description: 'Keep only 3 latest images'
        }
      ]
    });

    // Build and push the Docker image
    const dockerAsset = new ecr_assets.DockerImageAsset(this, 'ProcessingImage', {
      directory: path.join(__dirname, '../docker/sagemaker-processing'),
      platform: ecr_assets.Platform.LINUX_AMD64,
    });

    // Store the image URI for reference
    this.imageUri = dockerAsset.imageUri;

    // Output the image URI
    new cdk.CfnOutput(this, 'ProcessingImageUri', {
      value: this.imageUri,
      description: 'URI of the SageMaker processing image',
      exportName: 'ProcessingImageUri',
    });
  }
}