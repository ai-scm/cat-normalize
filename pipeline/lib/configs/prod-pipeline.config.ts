import { PipelineConfig } from './pipeline-config.interface';

/**
 * Production Environment Pipeline Configuration
 * 
 * IMPORTANT: Before deploying, you must:
 * 1. Create a GitHub connection in AWS Console:
 *    - Go to AWS CodePipeline → Settings → Connections
 *    - Click "Create connection"
 *    - Select "GitHub" and follow the authorization flow
 * 2. Copy the connection ARN and paste it in the connectionArn field below
 * 3. Update notification emails for production alerts
 */
export const prodPipelineConfig: PipelineConfig = {
  environment: 'prod',
  
  // AWS Configuration
  awsRegion: 'us-east-1',
  
  // Pipeline Configuration
  pipelineName: 'cat-prod-normalize-cicd-pipeline',
  artifactBucketName: 'cat-prod-normalize-pipeline-artifacts',
  requireManualApproval: true, // IMPORTANT: Manual approval required in production
  
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
      'team-lead@example.com', // TODO: Add your team lead email
      'devops@example.com', // TODO: Add DevOps team email
      // Add more emails as needed
    ]
  },
  
  // Tags
  tags: {
    Environment: 'prod',
    Project: 'CAT-CICD',
    ManagedBy: 'CDK',
    ProjectId: 'P2124',
    Purpose: 'infrastructure-deployment',
    CostCenter: 'engineering',
    Compliance: 'required',
    BackupPolicy: 'daily'
  }
};
