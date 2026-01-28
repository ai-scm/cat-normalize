/**
 * Configuration interface for ETL Stack (ETL-2)
 */
export interface EtlConfig {
  // Environment
  environment: 'test' | 'prod';
  
  // AWS Configuration
  awsRegion: string;
  accountId?: string;
  
  // S3 Configuration
  dataBucketName: string;
  cleanPrefix: string;
  curatedPrefix: string;
  athenaResultsPrefix: string;
  
  // Glue Configuration
  glue: {
    scriptS3Uri?: string;
    version: string;
    numberOfWorkers: number;
    workerType: "G.1X";
    jobName?: string;
  };
  
  // Catalog Configuration
  catalog: {
    databaseName: string;
    tableName?: string;
  };
  
  // Athena Configuration
  athena: {
    workGroupName: string;
    outputLocation?: string;
  };
  
  // Tags
  tags: Record<string, string>;
}