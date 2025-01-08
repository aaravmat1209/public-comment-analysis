import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

interface TestLambdaStackProps extends cdk.StackProps {
  apiEndpoint: string;
  webSocketEndpoint: string;
}

export class TestLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TestLambdaStackProps) {
    super(scope, id, props);

    // Create test Lambda
    const testFunction = new lambda.Function(this, 'TestFunction', {
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset('lambda/test-function', {
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: [
            'bash', '-c',
            'pip install -r requirements.txt -t /asset-output && cp index.py /asset-output'
          ]
        }
      }),
      handler: 'index.lambda_handler',
      timeout: cdk.Duration.minutes(10),
      memorySize: 256,
      environment: {
        API_ENDPOINT: props.apiEndpoint,
        WEBSOCKET_ENDPOINT: props.webSocketEndpoint
      }
    });

    // Add CloudWatch Logs permissions
    testFunction.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents'
      ],
      resources: ['*']
    }));
  }
}