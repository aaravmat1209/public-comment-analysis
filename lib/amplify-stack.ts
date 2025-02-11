import * as cdk from 'aws-cdk-lib';
import * as amplify from '@aws-cdk/aws-amplify-alpha';
import * as codebuild from 'aws-cdk-lib/aws-codebuild';
import { Construct } from 'constructs';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

interface AmplifyStackProps extends cdk.StackProps {
  apiUrl: string;
  webSocketEndpoint: string;
  owner: string;
  repository: string;
}

export class AmplifyStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: AmplifyStackProps) {
    super(scope, id, props);

    const owner = props.owner
    const repository = props.repository
    
    const githubTokenSecret = secretsmanager.Secret.fromSecretNameV2(
      this, 
      'GitHubToken', 
      'github-token'
    );
    
    // Create the Amplify application
    const amplifyApp = new amplify.App(this,'USDACommentAnalysis', {
      sourceCodeProvider: new amplify.GitHubSourceCodeProvider({
        owner,
        repository,
        oauthToken: githubTokenSecret.secretValue,
      }),
      appName: 'USDA-Comment-Analysis',
      
      // Configure build settings
      buildSpec: codebuild.BuildSpec.fromObjectToYaml({
        version: '1.0',
        frontend: {
          phases: {
            preBuild: {
              commands: [
                'cd frontend',
                'npm ci'
              ]
            },
            build: {
              commands: [
                'npm run build'
              ]
            }
          },
          artifacts: {
            baseDirectory: 'frontend/dist',
            files: [
              '**/*'
            ]
          },
          cache: {
            paths: [
              'frontend/node_modules/**/*'
            ]
          }
        }
      }),
    });

    // Add a branch
    const main = amplifyApp.addBranch('main', {
      autoBuild: true,
    });

    amplifyApp.addEnvironment('VITE_WEBSOCKET_URL', props.webSocketEndpoint);
    amplifyApp.addEnvironment('VITE_RESTAPI_URL', props.apiUrl);
    amplifyApp.addEnvironment('REACT_APP_REGION', this.region);
    main.addEnvironment('STAGE', 'prod');

    // Grant Amplify permission to read the GitHub token secret
    githubTokenSecret.grantRead(amplifyApp);

    // Output Amplify App URL
    new cdk.CfnOutput(this, 'AmplifyAppURL', {
      value: `https://${main.branchName}.${amplifyApp.defaultDomain}`,
    });
  }
}