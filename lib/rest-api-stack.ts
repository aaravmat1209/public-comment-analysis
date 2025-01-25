import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';

interface RestApiStackProps extends cdk.StackProps {
  stateMachine: sfn.StateMachine;
  stateTable: dynamodb.Table;
  webSocketEndpoint: string;
  clusteringBucketName: string;  // Added clustering bucket name
}

export class RestApiStack extends cdk.Stack {
    public readonly apiUrl: string;
    
    constructor(scope: Construct, id: string, props: RestApiStackProps) {
        super(scope, id, props);

        // Create REST API
        const api = new apigateway.RestApi(this, 'DocumentProcessorApi', {
            description: 'API for document processing',
            deployOptions: {
                stageName: 'dev',
                tracingEnabled: true,
            },
            defaultCorsPreflightOptions: {
                allowOrigins: apigateway.Cors.ALL_ORIGINS,
                allowMethods: apigateway.Cors.ALL_METHODS,
                allowHeaders: ['Content-Type', 'Authorization'],
            }
        });
        
        this.apiUrl = api.url;

        // Create Lambda for handling document submissions
        const submissionHandler = new lambda.Function(this, 'SubmissionHandler', {
            runtime: lambda.Runtime.PYTHON_3_9,
            code: lambda.Code.fromAsset('lambda/submission-handler'),
            handler: 'index.lambda_handler',
            environment: {
                STATE_MACHINE_ARN: props.stateMachine.stateMachineArn,
                STATE_TABLE_NAME: props.stateTable.tableName,
                WEBSOCKET_API_ENDPOINT: props.webSocketEndpoint,
                CLUSTERING_BUCKET: props.clusteringBucketName,  // Added clustering bucket env var
            },
            timeout: cdk.Duration.minutes(1),
        });

        // Grant permissions to access clustering bucket
        const clusteringBucketPolicy = new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
                's3:GetObject',
                's3:ListBucket'
            ],
            resources: [
                `arn:aws:s3:::${props.clusteringBucketName}`,
                `arn:aws:s3:::${props.clusteringBucketName}/*`
            ]
        });
        submissionHandler.addToRolePolicy(clusteringBucketPolicy);

        // Grant permissions to start state machine executions
        props.stateMachine.grantStartExecution(submissionHandler);
        props.stateTable.grantReadWriteData(submissionHandler);

        // Create API endpoints
        const documents = api.root.addResource('documents');
        
        // POST /documents - Submit new documents for processing
        documents.addMethod('POST', new apigateway.LambdaIntegration(submissionHandler));
        
        // GET /documents/{documentId} - Get document status and analysis
        const document = documents.addResource('{documentId}');
        document.addMethod('GET', new apigateway.LambdaIntegration(submissionHandler));

        // Output the API URL
        new cdk.CfnOutput(this, 'ApiUrl', {
            value: this.apiUrl,
            description: 'URL of the REST API',
            exportName: 'DocumentProcessorApiUrl',
        });
    }
}