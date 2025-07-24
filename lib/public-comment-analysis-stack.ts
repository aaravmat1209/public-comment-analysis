import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';

export interface PublicCommentAnalysisStackProps extends cdk.StackProps {
  apiKeySecretName: string;
  maxConcurrentWorkers?: number;
  lambdaMemorySize?: number;
  maxTimeout?: cdk.Duration;
  apiRateLimit: string;
  clusteringBucketName?: string;  // Optional during first deployment
}

export class PublicCommentAnalysisStack extends cdk.Stack {
  public readonly stateMachine: sfn.StateMachine;
  public readonly stateTable: dynamodb.Table;
  public readonly outputBucketName: string;

  constructor(scope: Construct, id: string, props: PublicCommentAnalysisStackProps) {
    super(scope, id, props);

    // Set default values
    // const lambdaMemorySize = props.lambdaMemorySize || 1024;
    const lambdaMemorySize = 3008; // Increased memory for better performance
    const maxTimeout = props.maxTimeout || cdk.Duration.minutes(15);

    // Create S3 bucket for storing processed comments
    const commentsBucket = new s3.Bucket(this, 'ProcessedCommentsBucket', {
      bucketName: `processed-comments-${this.account}-${this.region}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: cdk.Duration.days(90),
            },
          ],
        },
      ],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      versioned: true,
    });

    // Store bucket name as public property
    this.outputBucketName = commentsBucket.bucketName;

    // Create DynamoDB table for processing state
    this.stateTable = new dynamodb.Table(this, 'ProcessingStateTable', {
      partitionKey: { name: 'documentId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'chunkId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: 'ttl',
      pointInTimeRecovery: true, 
    });

    // Create Secrets Manager secret for the API key
    const apiKeySecret = new secretsmanager.Secret(this, 'RegulationsGovApiKey', {
      secretName: props.apiKeySecretName,
      description: 'API Key for regulations.gov API',
    });

    // Create base Lambda role
    const baseLambdaRole = new iam.Role(this, 'CommentProcessingLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: 'Base role for comment processor Lambda functions',
    });

    // Add required policies to Lambda role
    baseLambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );    
    apiKeySecret.grantRead(baseLambdaRole);
    commentsBucket.grantReadWrite(baseLambdaRole);
    this.stateTable.grantReadWriteData(baseLambdaRole);
    
    // Allow the Lambda role to invoke other Lambda functions (for progress tracker)
    baseLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [
        `arn:aws:lambda:${this.region}:${this.account}:function:PublicCommentAnalysis-ProgressTrackerHandler`,
        `arn:aws:lambda:${this.region}:${this.account}:function:WebSocketStack-ProgressTrackerHandler*`
      ],
    }));
    
    // Add clustering bucket permissions if provided
    if (props.clusteringBucketName) {
      const clusteringBucket = s3.Bucket.fromBucketName(
        this,
        'ClusteringBucket',
        props.clusteringBucketName
      );
      clusteringBucket.grantReadWrite(baseLambdaRole);
    }

    // Create CloudWatch Log Group
    const logGroup = new logs.LogGroup(this, 'PublicCommentAnalysisLogs', {
      logGroupName: '/aws/public-comment-analysis',
      retention: logs.RetentionDays.TWO_WEEKS,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Common Lambda configuration
    const lambdaConfig = {
      runtime: lambda.Runtime.PYTHON_3_9,
      role: baseLambdaRole,
      memorySize: lambdaMemorySize,
      timeout: maxTimeout,
      environment: {
        REGULATIONS_GOV_API_KEY_SECRET_ARN: apiKeySecret.secretArn,
        OUTPUT_S3_BUCKET: commentsBucket.bucketName,
        STATE_TABLE_NAME: this.stateTable.tableName,
        API_RATE_LIMIT: props.apiRateLimit,
        CLUSTERING_BUCKET: props.clusteringBucketName || '',
      },
      logGroup: logGroup,
      tracing: lambda.Tracing.ACTIVE, // Enable X-Ray tracing
    };

    // Create Lambda functions
    const initializer = new lambda.Function(this, 'InitializerFunction', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/initializer'),
      handler: 'index.lambda_handler',
      description: 'Initializes the comment processing workflow',
    });

    const workRangeCalculator = new lambda.Function(this, 'WorkRangeCalculator', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/work-range-calculator'),
      handler: 'index.lambda_handler',
      description: 'Calculates work ranges for parallel processing',
    });

    const processor = new lambda.Function(this, 'ProcessorFunction', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/processor'),
      handler: 'index.lambda_handler',
      description: 'Processes a range of comments',
    });

    const batchChecker = new lambda.Function(this, 'BatchCheckerFunction', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/batch-checker'),
      handler: 'index.lambda_handler',
      description: 'Checks if there are more batches to process',
    });

    const combiner = new lambda.Function(this, 'CombinerFunction', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/combiner'),
      handler: 'index.lambda_handler',
      description: 'Combines processed comments into final output',
      environment: {
        ...lambdaConfig.environment,
        CLUSTERING_BUCKET: props.clusteringBucketName || '',
        CONNECTIONS_TABLE_NAME: 'WebSocketStack-ConnectionsTable',
        WEBSOCKET_API_ENDPOINT: '',  // Will be populated by WebSocketStack
        API_GATEWAY_ENDPOINT: '',     // Will be populated by WebSocketStack
      }
    });

    // Create Step Functions state machine
    const initializeStep = new tasks.LambdaInvoke(this, 'Initialize', {
      lambdaFunction: initializer,
      resultPath: '$.initResult',
      retryOnServiceExceptions: true,
    });

    const calculateWorkRangesStep = new tasks.LambdaInvoke(this, 'CalculateWorkRanges', {
      lambdaFunction: workRangeCalculator,
      inputPath: '$.initResult.Payload',
      resultPath: '$.workBatches',
      retryOnServiceExceptions: true,
    });

    // Initialize processing state
    const initializeState = new sfn.Pass(this, 'InitializeState', {
      parameters: {
        'currentBatch': 0,
        'lastModifiedDate': '',
        'documentId.$': '$.initResult.Payload.documentId',
        'objectId.$': '$.initResult.Payload.objectId',
        'totalComments.$': '$.initResult.Payload.totalComments',
        'workBatches.$': '$.workBatches',
        'totalBatches.$': '$.workBatches.Payload.totalBatches',
        'expectedSets.$': '$.workBatches.Payload.expectedSets'
      }
    });

    // Get current batch of workers
    const getBatchStep = new sfn.Pass(this, 'GetCurrentBatch', {
      parameters: {
        'currentBatchWorkers.$': 'States.ArrayGetItem($.workBatches.Payload.batches[*].workers, $.currentBatch)',
        'currentBatch.$': '$.currentBatch',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments',
        'lastModifiedDate.$': '$.lastModifiedDate',
        'totalBatches.$': '$.workBatches.Payload.totalBatches',
        'expectedSets.$': '$.workBatches.Payload.expectedSets',
        'workBatches.$': '$.workBatches'
      }
    });

    // Process batch with concurrent workers
    const processBatchStep = new sfn.Map(this, 'ProcessBatch', {
      // maxConcurrency: 2,
      maxConcurrency:  6,
      itemsPath: sfn.JsonPath.stringAt('$.currentBatchWorkers'),
      parameters: {
        'workRange.$': '$$.Map.Item.Value',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'lastModifiedDate.$': '$.lastModifiedDate',
        'totalComments.$': '$.totalComments'
      },
      resultPath: '$.batchResults'
    }).iterator(new tasks.LambdaInvoke(this, 'ProcessTimeRange', {
      lambdaFunction: processor,
      retryOnServiceExceptions: true,
    }));

    const checkBatchProgressStep = new tasks.LambdaInvoke(this, 'CheckBatchProgress', {
      lambdaFunction: batchChecker,
      resultPath: '$.batchCheck',
      retryOnServiceExceptions: true,
    });

    // Update state with processing results
    const updateState = new sfn.Pass(this, 'UpdateState', {
      parameters: {
        'lastModifiedDate.$': "States.ArrayGetItem($.batchResults[*].Payload.lastProcessedDate, States.MathAdd(States.ArrayLength($.batchResults), -1))",
        'currentBatch.$': 'States.MathAdd($.currentBatch, 1)',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments',
        'workBatches.$': '$.workBatches',
      }
    });

    // Wait state for rate limiting
    const waitState = new sfn.Wait(this, 'WaitForRateLimit', {
      time: sfn.WaitTime.duration(cdk.Duration.minutes(60))
    });

    // Check progress and determine next action
    const checkProgressStep = new sfn.Choice(this, 'CheckProgress')
      .when(
        sfn.Condition.booleanEquals('$.batchCheck.Payload.hasMoreBatches', true),
        waitState
          .next(updateState)
          .next(getBatchStep)
      )
      .otherwise(
        new tasks.LambdaInvoke(this, 'CombineResults', {
          lambdaFunction: combiner,
          retryOnServiceExceptions: true
        })
      );

      // Define the main processing loop
      const processingLoop = getBatchStep
        .next(processBatchStep)
        .next(checkBatchProgressStep)
        .next(checkProgressStep);

    // Define the complete state machine
    const definition = initializeStep
      .next(calculateWorkRangesStep)
      .next(initializeState)
      .next(processingLoop);

    // Create the state machine
    this.stateMachine = new sfn.StateMachine(this, 'CommentProcessorStateMachine', {
      definition,
      timeout: cdk.Duration.days(7),
      tracingEnabled: true,
      logs: {
        destination: logGroup,
        level: sfn.LogLevel.ALL,
      },
    });

    // Create CloudWatch Dashboard
    const dashboard = new cloudwatch.Dashboard(this, 'CommentProcessorDashboard', {
      dashboardName: 'CommentProcessorMetrics',
    });

    // Add metrics to dashboard
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'Lambda Invocations',
        left: [
          initializer.metric('Invocations'),
          workRangeCalculator.metric('Invocations'),
          processor.metric('Invocations'),
          combiner.metric('Invocations'),
        ],
      }),
      new cloudwatch.GraphWidget({
        title: 'Lambda Errors',
        left: [
          initializer.metric('Errors'),
          workRangeCalculator.metric('Errors'),
          processor.metric('Errors'),
          combiner.metric('Errors'),
        ],
      }),
      new cloudwatch.GraphWidget({
        title: 'State Machine Executions',
        left: [
          this.stateMachine.metricSucceeded(),
          this.stateMachine.metricFailed(),
          this.stateMachine.metricTimedOut(),
        ],
      })
    );

    // Create CloudWatch Alarms
    new cloudwatch.Alarm(this, 'StateMachineFailureAlarm', {
      metric: this.stateMachine.metricFailed(),
      threshold: 1,
      evaluationPeriods: 1,
      alarmDescription: 'Alert when State Machine execution fails',
    });

    // Outputs
    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: this.stateMachine.stateMachineArn,
      description: 'ARN of the Step Functions state machine',
      exportName: 'CommentProcessorStateMachineArn',
    });

    new cdk.CfnOutput(this, 'OutputBucketName', {
      value: commentsBucket.bucketName,
      description: 'Name of the S3 bucket storing processed comments',
      exportName: 'CommentProcessorBucketName',
    });

    new cdk.CfnOutput(this, 'StateTableName', {
      value: this.stateTable.tableName,
      description: 'Name of the DynamoDB state table',
      exportName: 'CommentProcessorStateTableName',
    });
  }
}