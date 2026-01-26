import { NormalizeConfig } from './normalize-config.interface';

/**
 * Test Environment Configuration for Normalize Stack (ETL-1)
 */
export const testNormalizeConfig: NormalizeConfig = {
  environment: 'test',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // S3 Configuration
  s3BucketName: 'cat-test-normalize-reports',
  s3Prefix: 'reports/etl-process1/',
  
  // DynamoDB Configuration
  dynamoTableName: 'cat-dev-catia-conversations-table',
  
  // Lambda Configuration
  lambda: {
    name: 'cat-test-lambda-normalize',
    handler: 'lambda_function.lambda_handler',
    timeout: 900, // 15 minutes
    memorySize: 1024,
    runtime: 'python3.9',
    layerArn: 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13',
    codePath: 'lambda/etl-process1'
  },
  
  // EventBridge Schedule Configuration
  schedule: {
    enabled: false, // Disabled in test, manual execution
    cronExpression: 'cron(30 4 * * ? *)', // 4:30 AM UTC (11:30 PM Colombia)
    description: 'Daily ETL at 11:30 PM Colombia (4:30 AM UTC) - TEST'
  },
  
  // S3 Lifecycle Configuration
  lifecycle: {
    enabled: true,
    noncurrentVersionExpiration: 30
  },
  
  // Environment Variables for Lambda
  environmentVariables: {
    projectId: 'P2124',
    client: 'CAT'
  },
  
  // Tags
  tags: {
    Environment: 'test',
    stack: 'etl-process1',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
  }
};