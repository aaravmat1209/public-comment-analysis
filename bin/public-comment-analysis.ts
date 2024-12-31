import * as cdk from 'aws-cdk-lib';
import { PublicCommentAnalysisStack } from '../lib/public-comment-analysis-stack';

const app = new cdk.App();

new PublicCommentAnalysisStack(app, 'PublicCommentAnalysisStack', {
  apiKeySecretName: 'regulations-gov-api-key',
  maxConcurrentWorkers: 4,
  lambdaMemorySize: 1024,
  maxTimeout: cdk.Duration.minutes(15),
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tags: {
    Project: 'USDA Comment Processing',
    Environment: 'Development',
  },
});

app.synth();