import { NormalizeConfig } from './normalize-config.interface';

/**
 * Production Environment Configuration for Normalize Stack (ETL-1)
 */
export const prodNormalizeConfig: NormalizeConfig = {
  environment: 'prod',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // S3 Configuration
  s3BucketName: 'cat-prod-normalize-reports',
  s3Prefix: 'reports/etl-process1/',
  
  // DynamoDB Configuration
  dynamoTableName: 'cat-prod-catia-conversations-table',
  
  // Lambda Configuration
  lambda: {
    name: 'cat-prod-lambda-normalize',
    handler: 'lambda_function.lambda_handler',
    timeout: 900, // 15 minutes
    memorySize: 1024,
    runtime: 'python3.9',
    layerArn: 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:13',
    codePath: 'lambda/etl-process1'
  },
  
  // EventBridge Schedule Configuration
  schedule: {
    enabled: true, // Enabled in production
    cronExpression: 'cron(30 4 * * ? *)', // 4:30 AM UTC (11:30 PM Colombia)
    description: 'Daily ETL at 11:30 PM Colombia (4:30 AM UTC)'
  },
  
  // S3 Lifecycle Configuration
  lifecycle: {
    enabled: true,
    noncurrentVersionExpiration: 30
  },
  
  // Environment Variables for Lambda
  environmentVariables: {
    projectId: 'P0260',
    client: 'CAT'
  },
  
  // Tags
  tags: {
    Environment: 'prod',
    Project: 'CAT-NORMALIZE',
    Component: 'etl-process1',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',

    
  }
};