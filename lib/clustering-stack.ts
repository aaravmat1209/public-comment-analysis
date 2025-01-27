import * as cdk from 'aws-cdk-lib';
import * as apigateway from '@aws-cdk/aws-apigatewayv2-alpha';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';

export interface ClusteringStackProps extends cdk.StackProps {
  outputBucketName: string;
  stateMachineArn: string;
  stateTable: dynamodb.Table;  // Add state table
  webSocketEndpoint: string;  // Add WebSocket endpoint
  apiGatewayEndpoint: string;
  connectionsTable: dynamodb.Table;  // Add connections table
  webSocketApi: apigateway.WebSocketApi;
  stageName: string; 
}

export class ClusteringStack extends cdk.Stack {
  public readonly clusteringBucket: s3.Bucket;
  public readonly clusteringBucketName: string;

  constructor(scope: Construct, id: string, props: ClusteringStackProps) {
    super(scope, id, props);

    // Create S3 bucket for clustering pipeline
    this.clusteringBucket = new s3.Bucket(this, 'ClusteringBucket', {
      bucketName: `clustering-${this.account}-${this.region}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      lifecycleRules: [
        {
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.clusteringBucketName = this.clusteringBucket.bucketName;

    // Upload processing script to S3
    new s3deploy.BucketDeployment(this, 'ProcessingScriptDeployment', {
      sources: [s3deploy.Source.asset('scripts/clustering')],
      destinationBucket: this.clusteringBucket,
      destinationKeyPrefix: 'process',
    });

    // Import existing SageMaker role
    const sagemakerRole = iam.Role.fromRoleArn(
      this,
      'SageMakerExecutionRole',
      'arn:aws:iam::904233123149:role/sagemaker-processing',
      {
        mutable: false
      }
    );

    // Grant S3 permissions to SageMaker role
    this.clusteringBucket.grantRead(sagemakerRole);
    
    // Grant ListBucket permission explicitly
    sagemakerRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket'],
      resources: [this.clusteringBucket.bucketArn],
    }));

    // Add ECR permissions to SageMaker role
    sagemakerRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetAuthorizationToken'
      ],
      resources: ['*']  // GetAuthorizationToken requires * resource
    }));

    // Add specific repository permissions
    sagemakerRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:BatchCheckLayerAvailability',
        'ecr:BatchGetImage',
        'ecr:GetDownloadUrlForLayer'
      ],
      resources: [`arn:aws:ecr:${this.region}:${this.account}:repository/sagemaker-processing-image`]
    }));

    const sagemakerPolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'sagemaker:CreateProcessingJob',
        'sagemaker:DescribeProcessingJob',
        'sagemaker:StopProcessingJob',
        'sagemaker:ListProcessingJobs',
        'sagemaker:AddTags',  // Add permission for tagging
        'sagemaker:DeleteTags'
      ],
      resources: [
        `arn:aws:sagemaker:${this.region}:${this.account}:processing-job/*`
      ]
    });

    // Create base Lambda role
    const baseLambdaRole = new iam.Role(this, 'ClusteringLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: 'Base role for clustering Lambda functions',
    });

    // Add required policies to Lambda role
    baseLambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );
    this.clusteringBucket.grantReadWrite(baseLambdaRole);
    
    // Add SageMaker permissions
    baseLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sagemaker:CreateProcessingJob'],
      resources: ['*'],
    }));

    // Add PassRole permission for SageMaker role
    baseLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['iam:PassRole'],
      resources: [sagemakerRole.roleArn],
      conditions: {
        'StringLike': {
          'iam:PassedToService': 'sagemaker.amazonaws.com'
        }
      }
    }));

    // Add Bedrock permissions
    baseLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*'],
    }));

    const webSocketLayer = new lambda.LayerVersion(this, 'WebSocketLayer', {
      code: lambda.Code.fromAsset('lambda/layers/websocket'),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      description: 'WebSocket utilities layer',
    });

    // Create processing Lambda
    const processingLambda = new lambda.Function(this, 'ProcessingLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda/clustering-processor'),
      handler: 'index.lambda_handler',
      role: baseLambdaRole,
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        IMAGE_URI: '904233123149.dkr.ecr.us-west-2.amazonaws.com/sagemaker-processing-image:latest',
        ROLE_ARN: sagemakerRole.roleArn,
        STATE_TABLE_NAME: props.stateTable.tableName,  // Add state table name
        WEBSOCKET_API_ENDPOINT: props.webSocketEndpoint,  // Add WebSocket endpoint
        API_GATEWAY_ENDPOINT: props.apiGatewayEndpoint,
        CONNECTIONS_TABLE_NAME: props.connectionsTable.tableName  // Add connections table name
      },
      layers: [webSocketLayer],  // Add WebSocket layer
    });

    processingLambda.addToRolePolicy(sagemakerPolicy);
    
    props.stateTable.grantReadWriteData(processingLambda);
    props.connectionsTable.grantReadWriteData(processingLambda);

    // Explicit permissions for Scan operation
    processingLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:Scan',
        'dynamodb:UpdateItem'
      ],
      resources: [
        props.connectionsTable.tableArn,
        props.stateTable.tableArn
      ]
    }));

    // Add WebSocket management permissions to Lambda role
    processingLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'execute-api:ManageConnections',
        'execute-api:Invoke'
      ],
      resources: [
        // Specific POST permission for managing connections
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/POST/@connections/*`,
        // Specific GET permission for getting connection info
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/GET/@connections/*`,
        // Specific DELETE permission for cleaning up connections
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/DELETE/@connections/*`,
        // General permissions for the stage
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/*`
      ]
    }));

    // Create analysis Lambda
    // Create a layer for the Python dependencies
    const analysisLayer = new lambda.LayerVersion(this, 'AnalysisLayer', {
      code: lambda.Code.fromAsset('lambda/clustering-analyzer', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output/python'
          ]
        }
      }),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      description: 'Dependencies for analysis lambda',
    });

    const analysisLambda = new lambda.Function(this, 'AnalysisLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda/clustering-analyzer'),
      handler: 'index.lambda_handler',
      role: baseLambdaRole,
      layers: [webSocketLayer, analysisLayer],
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        STATE_TABLE_NAME: props.stateTable.tableName,
        WEBSOCKET_API_ENDPOINT: props.webSocketEndpoint,
        API_GATEWAY_ENDPOINT: props.apiGatewayEndpoint,
        CONNECTIONS_TABLE_NAME: props.connectionsTable.tableName
      },
    });

    props.stateTable.grantReadWriteData(analysisLambda);
    props.connectionsTable.grantReadWriteData(analysisLambda);

    // Explicit permissions for Scan operation
    analysisLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'dynamodb:Scan',
        'dynamodb:UpdateItem'
      ],
      resources: [
        props.connectionsTable.tableArn,
        props.stateTable.tableArn
      ]
    }));

    // Add WebSocket management permissions to Lambda role
    analysisLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'execute-api:ManageConnections',
        'execute-api:Invoke'
      ],
      resources: [
        // Specific POST permission for managing connections
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/POST/@connections/*`,
        // Specific GET permission for getting connection info
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/GET/@connections/*`,
        // Specific DELETE permission for cleaning up connections
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/DELETE/@connections/*`,
        // General permissions for the stage
        `arn:aws:execute-api:${this.region}:${this.account}:${props.webSocketApi.apiId}/${props.stageName}/*`
      ]
    }));

    // Add S3 triggers
    // Trigger processing Lambda when files are added to before-clustering/
    this.clusteringBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(processingLambda),
      { 
        prefix: 'before-clustering/',
        suffix: '.csv'
      }
    );

    // Add environment variables for processing
    processingLambda.addEnvironment('CLUSTERING_BUCKET', this.clusteringBucketName);
    processingLambda.addEnvironment('INPUT_PREFIX', 'before-clustering');
    processingLambda.addEnvironment('OUTPUT_PREFIX', 'after-clustering');

    // Trigger analysis Lambda when files are added to after-clustering/
    this.clusteringBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(analysisLambda),
      { prefix: 'after-clustering/' }
    );

    // Add outputs
    new cdk.CfnOutput(this, 'ClusteringBucketName', {
      value: this.clusteringBucketName,
      description: 'Name of the clustering pipeline bucket',
      exportName: 'ClusteringBucketName',
    });
  }
}