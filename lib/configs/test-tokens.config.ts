import { Project } from 'aws-cdk-lib/aws-codebuild';
import { TokensConfig } from './tokens-config.interface';

/**
 * Test Environment Configuration for Tokens Processing
 */
export const testTokensConfig: TokensConfig = {
  environment: 'test',
  
  // DynamoDB Tables
  oldDynamoTableName: 'BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF',
  newDynamoTableName: 'cattest4-BedrockChatStack-DatabaseConversationTableV3C1D85773-1PPI6V82M1BMI',
  
  // S3 Configuration
  outputBucket: 'cat-test-normalize-reports',
  outputPrefix: 'tokens-analysis/',
  historicalPrefix: 'tokens-analysis/historical/',
  
  // Athena Configuration
  athenaDatabase: 'cat_test_analytics_db',
  athenaWorkgroup: 'wg-cat-test-analytics',
  athenaResultsLocation: 's3://cat-test-normalize-reports/athena/results/',
  
  // Archival Lambda Configuration (processes old table - one-time)
  archivalLambda: {
    name: 'cat-test-lambda-tokens-archival-processing',
    handler: 'lambda-tokens-archival-processing.lambda_handler',
    timeout: 900,  // 15 minutes
    memorySize: 1024,  // 1GB
    runtime: 'python3.11'
  },
  
  // Consolidated Lambda Configuration (processes new table - daily)
  consolidatedLambda: {
    name: 'cat-test-lambda-tokens',
    handler: 'tokens_lambda.lambda_handler',
    timeout: 300,  // 5 minutes
    memorySize: 512,  // 512MB
    runtime: 'python3.11'
  },
  
  // Lambda Layer
  layerName: 'cat-test-pandas-numpy-layer',
  
  // Date Filters
  dateFilter: {
    archivalStart: '2025-08-04',  // Start date for historical data
    archivalEnd: '2025-12-31',    // End date for historical data (before migration)
    consolidatedStart: '2026-01-01'  // Start date for new table data (migration date)
  },
  
  // EventBridge Schedule (disabled for test)
  schedule: {
    enabled: false,  // Don't auto-run in test
    cronExpression: 'cron(0 3 * * ? *)',  // 3 AM UTC (10 PM Colombia)
    description: 'Daily tokens processing - Test Environment (disabled by default)'
  },
  
  // AWS Region
  awsRegion: 'us-east-1',
  
  // Tags
  tags: {
    Environment: 'test',
    Project: 'CAT-TOKENS-ANALYSIS',
    Component: 'tokens-processing',
    ManagedBy: 'CDK',
    ProjectId: 'P2124'
  }
};