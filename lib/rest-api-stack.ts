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
    clusteringBucketName: string;
    outputBucketName?: string;
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
                allowOrigins: ['*'],
                allowMethods: ['GET', 'POST', 'OPTIONS'],
                allowHeaders: [
                    'Content-Type',
                    'Authorization',
                    'X-Amz-Date',
                    'X-Api-Key',
                    'X-Amz-Security-Token'
                ],
                maxAge: cdk.Duration.seconds(300)
            }
        });

        // Add Gateway Responses for all error types
        const corsHeaders = {
            'Access-Control-Allow-Origin': "'*'",
            'Access-Control-Allow-Headers': "'Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token'",
            'Access-Control-Allow-Methods': "'GET,POST,OPTIONS'"
        };

        // Add responses for all relevant gateway error types
        const gatewayResponses: { [key: string]: apigateway.GatewayResponse } = {
            UNAUTHORIZED: new apigateway.GatewayResponse(this, 'Unauthorized', {
                restApi: api,
                type: apigateway.ResponseType.UNAUTHORIZED,
                responseHeaders: corsHeaders
            }),
            ACCESS_DENIED: new apigateway.GatewayResponse(this, 'AccessDenied', {
                restApi: api,
                type: apigateway.ResponseType.ACCESS_DENIED,
                responseHeaders: corsHeaders
            }),
            RESOURCE_NOT_FOUND: new apigateway.GatewayResponse(this, 'ResourceNotFound', {
                restApi: api,
                type: apigateway.ResponseType.RESOURCE_NOT_FOUND,
                responseHeaders: corsHeaders
            }),
            INVALID_API_KEY: new apigateway.GatewayResponse(this, 'InvalidApiKey', {
                restApi: api,
                type: apigateway.ResponseType.INVALID_API_KEY,
                responseHeaders: corsHeaders
            }),
            REQUEST_TOO_LARGE: new apigateway.GatewayResponse(this, 'RequestTooLarge', {
                restApi: api,
                type: apigateway.ResponseType.REQUEST_TOO_LARGE,
                responseHeaders: corsHeaders
            }),
            THROTTLED: new apigateway.GatewayResponse(this, 'Throttled', {
                restApi: api,
                type: apigateway.ResponseType.THROTTLED,
                responseHeaders: corsHeaders
            }),
            DEFAULT_4XX: new apigateway.GatewayResponse(this, 'Default4XX', {
                restApi: api,
                type: apigateway.ResponseType.DEFAULT_4XX,
                responseHeaders: corsHeaders
            }),
            DEFAULT_5XX: new apigateway.GatewayResponse(this, 'Default5XX', {
                restApi: api,
                type: apigateway.ResponseType.DEFAULT_5XX,
                responseHeaders: corsHeaders
            })
        };
        
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
                CLUSTERING_BUCKET: props.clusteringBucketName,
                OUTPUT_S3_BUCKET: props.outputBucketName || '',
            },
            timeout: cdk.Duration.minutes(1),
        });

        // Grant permissions
        props.stateMachine.grantStartExecution(submissionHandler);
        props.stateTable.grantReadWriteData(submissionHandler);

        // Add SageMaker permissions for checking job status
        submissionHandler.addToRolePolicy(new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
                'sagemaker:ListProcessingJobs',
                'sagemaker:DescribeProcessingJob',
                'sagemaker:StopProcessingJob'
            ],
            resources: ['*']
        }));

        if (props.clusteringBucketName) {
            const clusteringBucketPolicy = new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['s3:GetObject', 's3:ListBucket'],
                resources: [
                    `arn:aws:s3:::${props.clusteringBucketName}`,
                    `arn:aws:s3:::${props.clusteringBucketName}/*`
                ]
            });
            submissionHandler.addToRolePolicy(clusteringBucketPolicy);
        }

        if (props.outputBucketName) {
            const outputBucketPolicy = new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: ['s3:GetObject', 's3:ListBucket'],
                resources: [
                    `arn:aws:s3:::${props.outputBucketName}`,
                    `arn:aws:s3:::${props.outputBucketName}/*`
                ]
            });
            submissionHandler.addToRolePolicy(outputBucketPolicy);
        }

        // Create API endpoints
        const documents = api.root.addResource('documents');
        
        // POST /documents
        documents.addMethod('POST', new apigateway.LambdaIntegration(submissionHandler, {
            proxy: true
        }));
        
        // GET /documents/{documentId}
        const document = documents.addResource('{documentId}');
        document.addMethod('GET', new apigateway.LambdaIntegration(submissionHandler, {
            proxy: true
        }));

        // Output the API URL
        new cdk.CfnOutput(this, 'ApiUrl', {
            value: this.apiUrl,
            description: 'URL of the REST API',
            exportName: 'DocumentProcessorApiUrl',
        });
    }
}