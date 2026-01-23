import { EtlConfig } from './etl-config.interface';

/**
 * Production Environment Configuration for ETL Stack (ETL-2)
 */
export const prodEtlConfig: EtlConfig = {
  environment: 'prod',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // S3 Configuration
  dataBucketName: 'cat-prod-normalize-reports',
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
    databaseName: 'cat_prod_analytics_db'
  },
  
  // Athena Configuration
  athena: {
    workGroupName: 'wg-cat-prod-analytics'
  },
  
  // Tags
  tags: {
    Environment: 'prod',
    Project: 'CAT-NORMALIZE',
    Component: 'etl-process2',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
  }
};