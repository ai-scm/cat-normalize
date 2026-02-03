import { EtlConfig } from './etl-config.interface';

/**
 * Test Environment Configuration for ETL Stack (ETL-2)
 */
export const testEtlConfig: EtlConfig = {
  environment: 'test',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // S3 Configuration
  dataBucketName: 'cat-test-normalize-reports',
  cleanPrefix: 'reports/etl-process1/',
  curatedPrefix: 'reports/etl-process2/',
  athenaResultsPrefix: 'athena/results/',
  
  // Glue Configuration
  glue: {
    version: '4.0',
    numberOfWorkers: 2,
    workerType: 'G.1X'
  },

  lambda: {
    name: 'cat-test-feedback-processor',
    handler: 'lambda_function.lambda_handler',
    timeout: 300,
    memorySize: 512,
    runtime: 'python3.11',
    layerArn: 'arn:aws:lambda:us-east-1:123456789012:layer:AWSSDKPandasLayer:1',
    codePath: 'lambda/feedback-processor',
    dynamoDbTableName: 'cat-dev-catia-conversations-table',
    outputPrefix: 'reports/feedbacks/',
    timeoutMinutes: 5,
    scheduleExpression: 'rate(1 days)'
  },
  
  // Catalog Configuration
  catalog: {
    databaseName: 'cat_test_analytics_db'
  },
  
  // Athena Configuration
  athena: {
    workGroupName: 'wg-cat-test-analytics'
  },
  
  // Tags
  tags: {
    Environment: 'test',
    Stack: 'etl-process2',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
  }
};