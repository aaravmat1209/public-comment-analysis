import * as cdk from 'aws-cdk-lib';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as ecr_assets from 'aws-cdk-lib/aws-ecr-assets';
import * as path from 'path';
import { Construct } from 'constructs';

export class ECRStack extends cdk.Stack {
  public readonly repository: ecr.Repository;
  public readonly processingImage: ecr_assets.DockerImageAsset;
  
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create ECR repository
    this.repository = new ecr.Repository(this, 'SageMakerProcessingRepo', {
      repositoryName: `sagemaker-processing-image-${this.region}`,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          maxImageCount: 3,
          description: 'Keep only 3 latest images'
        }
      ]
    });

    // Build and push the Docker image
    this.processingImage = new ecr_assets.DockerImageAsset(this, 'ProcessingImage', {
      directory: path.join(__dirname, '../docker/sagemaker-processing'),
      platform: ecr_assets.Platform.LINUX_AMD64,
    });

    // Output the image URI
    new cdk.CfnOutput(this, 'ProcessingImageUri', {
      value: this.processingImage.imageUri,
      description: 'URI of the SageMaker processing image',
      exportName: 'ProcessingImageUri',
    });

    new cdk.CfnOutput(this, 'RepositoryName', {
      value: this.repository.repositoryName,
      description: 'Name of the ECR repository',
      exportName: 'ProcessingRepositoryName',
    });
  }
}