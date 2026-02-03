import { PipelineConfig } from './pipeline-config.interface';

export const prodPipelineConfig: PipelineConfig = {
  environment: 'prod',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // Pipeline Configuration
  pipelineName: 'cat-prod-normalize-cicd-pipeline',
  artifactBucketName: 'cat-prod-normalize-pipeline-artifacts',
  requireManualApproval: true, // Manual approval required for production
  
  // GitHub Configuration
 github: {
    owner: 'ai-scm', 
    repositoryName: 'cat-normalize',
    repositoryUrl: 'https://github.com/ai-scm/cat-normalize',
    branchName: 'main',
    connectionArn: 'arn:aws:codeconnections:us-east-1:081899001252:connection/11a44192-0d15-4d59-aafd-f5bd6919b6aa',
  },
  
  // Notifications (recommended for production)
  notifications: {
    enabled: false, // Enable notifications for production
    emails: [
      'julian.rincon@blend360.com', 'roger.bonilla@blend360.com'
    ]
  },
  
  // Tags
  tags: {
    Environment: 'prod',
    Project: 'cat-normalize-cicd',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
  }
};
