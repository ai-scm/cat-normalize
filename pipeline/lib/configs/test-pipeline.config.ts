import { PipelineConfig } from './pipeline-config.interface';

export const testPipelineConfig: PipelineConfig = {
  environment: 'test',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // Pipeline Configuration
  pipelineName: 'cat-test-normalize-cicd-pipeline',
  artifactBucketName: 'cat-test-normalize-pipeline-artifacts',
  requireManualApproval: false, // No manual approval in test
  
  // GitHub Configuration
  github: {
    owner: 'ai-scm', 
    repositoryName: 'cat-normalize',
    repositoryUrl: 'https://github.com/ai-scm/cat-normalize',
    branchName: 'test',
    connectionArn: 'arn:aws:codeconnections:us-east-1:081899001252:connection/11a44192-0d15-4d59-aafd-f5bd6919b6aa',
  },
  
  notifications: {
    enabled: false, // Enable if you want email notifications
    emails: [
      // 'your-email@example.com' // Add email addresses here
    ]
  },
  
  // Tags
  tags: {
    Environment: 'test',
    Project: 'cat-normalize-cicd',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
  }
};
