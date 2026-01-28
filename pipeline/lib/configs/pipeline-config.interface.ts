/**
 * Pipeline Configuration Interface
 * Defines the structure for CI/CD pipeline configuration
 */
export interface PipelineConfig {
  // Environment
  environment: 'test' | 'prod';
  
  // AWS Configuration
  awsRegion: string;
  
  // Pipeline Configuration
  pipelineName: string;
  artifactBucketName: string;
  requireManualApproval: boolean;
  
  // GitHub Configuration
  github: {
    owner: string;
    repositoryName: string;
    repositoryUrl?: string;
    branchName: string;
    connectionArn: string; // ARN of the CodeStar connection to GitHub
  };
  
  // Notification Configuration (optional)
  notifications?: {
    enabled: boolean;
    emails?: string[];
  };
  
  // Tags
  tags?: {
    [key: string]: string;
  };
}
