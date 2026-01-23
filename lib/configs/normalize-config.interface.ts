/**
 * Configuration interface for Normalize Stack (ETL-1)
 */
export interface NormalizeConfig {
  // Environment
  environment: 'test' | 'prod';
  
  // AWS Configuration
  awsRegion: string;
  accountId?: string;
  
  // S3 Configuration
  s3BucketName: string;
  s3Prefix?: string;
  
  // DynamoDB Configuration
  dynamoTableName: string;
  
  // Lambda Configuration
  lambda: {
    name: string;
    handler: string;
    timeout: number;
    memorySize: number;
    runtime: string;
    layerArn: string;
    codePath: string;
  };
  
  // EventBridge Schedule Configuration
  schedule: {
    enabled: boolean;
    cronExpression: string;
    description: string;
  };
  
  // S3 Lifecycle Configuration
  lifecycle?: {
    enabled: boolean;
    noncurrentVersionExpiration: number;
  };
  
  // Environment Variables for Lambda
  environmentVariables: {
    projectId: string;
    client: string;
    [key: string]: string;
  };
  
  // Tags
  tags: Record<string, string>;
}