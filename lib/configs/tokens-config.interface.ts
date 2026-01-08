/**
 * Tokens Stack Configuration Interface
 * Defines the structure for environment-specific configurations
 */
export interface TokensConfig {
  // Environment identifier
  environment: 'test' | 'prod';
  
  // DynamoDB Configuration
  oldDynamoTableName: string;  // Historical/archived table
  newDynamoTableName: string;  // Current active table
  
  // S3 Configuration
  outputBucket: string;
  outputPrefix: string;
  historicalPrefix: string;
  
  // Athena Configuration
  athenaDatabase: string;
  athenaWorkgroup: string;
  athenaResultsLocation: string;
  athenaViewName: string;
  
  // Lambda Configuration
  archivalLambda: {
    name: string;
    handler: string;
    timeout: number;  // in seconds
    memorySize: number;  // in MB
    runtime: string;  // e.g., 'python3.11'
  };
  
  consolidatedLambda: {
    name: string;
    handler: string;
    timeout: number;
    memorySize: number;
    runtime: string;
  };
  
  // Python Layer Configuration
  layerName: string;
  
  // Date Filters
  dateFilter: {
    archivalStart: string;  // Format: 'YYYY-MM-DD'
    archivalEnd: string;    // Format: 'YYYY-MM-DD'
    consolidatedStart: string;  // Format: 'YYYY-MM-DD'
  };
  
  // EventBridge Schedule Configuration
  schedule: {
    enabled: boolean;
    cronExpression: string;
    description: string;
  };
  
  // AWS Region
  awsRegion: string;
  
  // Optional: Tags
  tags?: Record<string, string>;
}