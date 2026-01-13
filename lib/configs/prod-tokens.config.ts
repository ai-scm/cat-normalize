import { TokensConfig } from './tokens-config.interface';

/**
 * Production Environment Configuration for Tokens Processing
 */
export const prodTokensConfig: TokensConfig = {
  environment: 'prod',
  
  // DynamoDB Tables
  oldDynamoTableName: 'BedrockChatStack-DatabaseConversationTable03F3FD7A-VCTDHISEE1NF',
  newDynamoTableName: 'catprod-BedrockChatStack-DatabaseConversationTableV3C1D85773-1CONFM1NXLZQ5',
  
  // S3 Configuration
  outputBucket: 'cat-prod-normalize-reports',
  outputPrefix: 'tokens-analysis/',
  historicalPrefix: 'archives/tokens-analysis/',
  
  // Athena Configuration
  athenaDatabase: 'cat_prod_analytics_db',
  athenaWorkgroup: 'wg-cat-prod-analytics',
  athenaResultsLocation: 's3://cat-prod-normalize-reports/athena/results/',
  athenaViewName: 'tokens_usage_analysis',
  
  // Archival Lambda Configuration (processes old table - one-time)
  archivalLambda: {
    name: 'cat-prod-lambda-tokens-archival-processing',
    handler: 'lambda-tokens-archival-processing.lambda_handler',
    timeout: 900,  // 15 minutes
    memorySize: 2048,  // 2GB for production (larger dataset)
    runtime: 'python3.11'
  },
  
  // Consolidated Lambda Configuration (processes new table - daily)
  consolidatedLambda: {
    name: 'cat-prod-lambda-tokens',
    handler: 'tokens_lambda.lambda_handler',
    timeout: 300,  // 5 minutes
    memorySize: 512,  // 512MB
    runtime: 'python3.11'
  },
  
  // Lambda Layer
  layerName: 'cat-prod-pandas-numpy-layer',
  
  // Date Filters
  dateFilter: {
    archivalStart: '2025-08-04',  // Start date for historical data
    archivalEnd: '2025-12-31',    // End date for historical data (before migration)
    consolidatedStart: '2026-01-01'  // Start date for new table data (migration date)
  },
  
  // EventBridge Schedule (enabled for production)
  schedule: {
    enabled: true,  // Auto-run daily in production
    cronExpression: 'cron(30 4 * * ? *)',  // 4:30 AM UTC (11:30 PM Colombia)
    description: 'Daily tokens processing at 11:30 PM Colombia (4:30 AM UTC)'
  },
  
  // AWS Region
  awsRegion: 'us-east-1',
  
  // Tags
  tags: {
    Environment: 'prod',
    Project: 'CAT-TOKENS-ANALYSIS',
    Component: 'tokens-processing',
    ManagedBy: 'CDK',
    ProjectId: 'P2124'

  }
};