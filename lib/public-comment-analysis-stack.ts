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
}

export class PublicCommentAnalysisStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: PublicCommentAnalysisStackProps) {
    super(scope, id, props);

    // Set default values
    const maxConcurrentWorkers = props.maxConcurrentWorkers || 4;
    const lambdaMemorySize = props.lambdaMemorySize || 1024;
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

    // Create DynamoDB table for processing state
    const stateTable = new dynamodb.Table(this, 'ProcessingStateTable', {
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
    stateTable.grantReadWriteData(baseLambdaRole);

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
        STATE_TABLE_NAME: stateTable.tableName,
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

    const combiner = new lambda.Function(this, 'CombinerFunction', {
      ...lambdaConfig,
      code: lambda.Code.fromAsset('lambda/combiner'),
      handler: 'index.lambda_handler',
      description: 'Combines processed comments into final output',
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

    // Calculate wait time based on batch size
    const calculateWaitTime = new sfn.Pass(this, 'CalculateWaitTime', {
      parameters: {
        'waitSeconds': 1920, // 32 minutes in seconds
        'currentBatch.$': '$.currentBatch',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments',
        'workBatches.$': '$.workBatches',
        'totalBatches.$': '$.totalBatches'
      }
    });

    // Add wait state to respect API rate limits
    const waitForRateLimit = new sfn.Wait(this, 'WaitForRateLimit', {
      time: sfn.WaitTime.secondsPath('$.waitSeconds')
    });

    // Get current batch of workers
    const getBatchStep = new sfn.Pass(this, 'GetCurrentBatch', {
      parameters: {
        'currentBatchWorkers.$': 'States.ArrayGetItem($.workBatches.Payload.batches[*].workers, $.currentBatch)',
        'currentBatch.$': '$.currentBatch',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments',
        'totalBatches.$': '$.workBatches.Payload.totalBatches',
        'workBatches.$': '$.workBatches'
      }
    });

    // Process a single batch of workers
    const processBatchStep = new sfn.Map(this, 'ProcessBatch', {
      maxConcurrency: 4,
      itemsPath: sfn.JsonPath.stringAt('$.currentBatchWorkers'),
      parameters: {
        'workRange.$': '$$.Map.Item.Value',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments'
      },
      resultPath: '$.batchResults'
    }).iterator(new tasks.LambdaInvoke(this, 'ProcessTimeRange', {
      lambdaFunction: processor,
      retryOnServiceExceptions: true,
    }));

    // Increment batch counter with preserved state
    const incrementBatchStep = new sfn.Pass(this, 'IncrementBatch', {
      parameters: {
        'currentBatch.$': 'States.MathAdd($.currentBatch, 1)',
        'documentId.$': '$.documentId',
        'objectId.$': '$.objectId',
        'totalComments.$': '$.totalComments',
        'workBatches.$': '$.workBatches',
        'totalBatches.$': '$.totalBatches'
      }
    });

    // Check if more batches to process
    const checkMoreBatchesStep = new sfn.Choice(this, 'MoreBatches')
      .when(
        sfn.Condition.numberLessThanJsonPath('$.currentBatch', '$.totalBatches'),
        calculateWaitTime
          .next(waitForRateLimit)
          .next(incrementBatchStep)
          .next(getBatchStep)
      )
      .otherwise(
        new tasks.LambdaInvoke(this, 'CombineResults', {
          lambdaFunction: combiner,
          retryOnServiceExceptions: true
        })
      );

    // Create the sequential batch processing loop
    const batchProcessingLoop = getBatchStep
      .next(processBatchStep)
      .next(checkMoreBatchesStep);

    // Define the main state machine
    const definition = initializeStep
      .next(calculateWorkRangesStep)
      .next(new sfn.Pass(this, 'InitializeBatchCounter', {
        parameters: {
          'currentBatch': 0,
          'documentId.$': '$.initResult.Payload.documentId',
          'objectId.$': '$.initResult.Payload.objectId',
          'totalComments.$': '$.initResult.Payload.totalComments',
          'workBatches.$': '$.workBatches',
          'totalBatches.$': '$.workBatches.Payload.totalBatches'
        }
      }))
      .next(batchProcessingLoop);

    const stateMachine = new sfn.StateMachine(this, 'CommentProcessorStateMachine', {
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
          stateMachine.metricSucceeded(),
          stateMachine.metricFailed(),
          stateMachine.metricTimedOut(),
        ],
      })
    );

    // Create CloudWatch Alarms
    new cloudwatch.Alarm(this, 'StateMachineFailureAlarm', {
      metric: stateMachine.metricFailed(),
      threshold: 1,
      evaluationPeriods: 1,
      alarmDescription: 'Alert when State Machine execution fails',
    });

    // Outputs
    new cdk.CfnOutput(this, 'StateMachineArn', {
      value: stateMachine.stateMachineArn,
      description: 'ARN of the Step Functions state machine',
      exportName: 'CommentProcessorStateMachineArn',
    });

    new cdk.CfnOutput(this, 'OutputBucketName', {
      value: commentsBucket.bucketName,
      description: 'Name of the S3 bucket storing processed comments',
      exportName: 'CommentProcessorBucketName',
    });

    new cdk.CfnOutput(this, 'StateTableName', {
      value: stateTable.tableName,
      description: 'Name of the DynamoDB state table',
      exportName: 'CommentProcessorStateTableName',
    });
  }
}