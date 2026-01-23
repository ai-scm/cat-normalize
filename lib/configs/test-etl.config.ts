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
    Environment: 'TEST',
    Project: 'CAT-NORMALIZE',
    Component: 'etl-process2',
    ManagedBy: 'CDK',
    CostCenter: 'DATA-ANALYTICS',
    ProjectId: 'P0260',
    Owner: 'DataEngineering',
    ETLComponent: 'ETL-2'
  }
};