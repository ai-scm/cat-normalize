import * as cdk from 'aws-cdk-lib';
import { PipelineStack } from '../lib/pipeline-stack';

// Import configurations
import { testPipelineConfig } from '../lib/configs/test-pipeline.config';
import { prodPipelineConfig } from '../lib/configs/prod-pipeline.config';

const app = new cdk.App();

// ==================== ENVIRONMENT SELECTION ====================
// Get environment from context or environment variable
// Usage: cdk deploy -c environment=test
//    or: ENVIRONMENT=test cdk deploy
const environment = app.node.tryGetContext('environment') || process.env.ENVIRONMENT || 'test';

console.log(`Deploying CI/CD Pipeline for environment: ${environment.toUpperCase()}`);

// Validate environment
if (environment !== 'test' && environment !== 'prod') {
  throw new Error(`Unknown environment: ${environment}. Use 'test' or 'prod'.`);
}

// ==================== SELECT CONFIGURATION ====================
const pipelineConfig = environment === 'prod' ? prodPipelineConfig : testPipelineConfig;

console.log(`Using ${environment} pipeline configuration`);
console.log(`   - Repository: ${pipelineConfig.github.repositoryName}`);
console.log(`   - Branch: ${pipelineConfig.github.branchName}`);
console.log(`   - Owner: ${pipelineConfig.github.owner}`);

// ==================== COMMON PROPS ====================
const commonEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: pipelineConfig.awsRegion
};

// ==================== PIPELINE STACK ====================
const pipelineStackName = `cat-${environment}-normalize-cicd-pipeline-stack`;

console.log(`\n Creating Pipeline Stack: ${pipelineStackName}`);
console.log(`   - Pipeline Name: ${pipelineConfig.pipelineName}`);
console.log(`   - Artifact Bucket: ${pipelineConfig.artifactBucketName}`);
console.log(`   - Manual Approval: ${pipelineConfig.requireManualApproval ? 'ENABLED' : 'DISABLED'}`);

const pipelineStack = new PipelineStack(
  app,
  pipelineStackName,
  pipelineConfig,
  {
    env: commonEnv,
    stackName: pipelineStackName,
    description: `CI/CD Pipeline Stack for ${environment} environment - Automates deployment of CAT ETL infrastructure`,
    tags: {
      ...pipelineConfig.tags,
      DeploymentDate: new Date().toISOString().split('T')[0],
      StackType: 'cicd-pipeline'
    }
  }
);

// ==================== APPLY GLOBAL TAGS ====================
cdk.Tags.of(app).add('Environment', environment.toUpperCase());
cdk.Tags.of(app).add('ManagedBy', 'CDK');
cdk.Tags.of(app).add('Project', 'CAT-CICD-PIPELINE');
cdk.Tags.of(app).add('PipelineType', 'infrastructure-deployment');

// ==================== SYNTHESIS ====================
app.synth();

console.log('\nCDK Pipeline app synthesized successfully');
console.log(`\nStack created: ${pipelineStackName}`);

console.log(`\nDeployment commands:`);
console.log(`\n   Deploy pipeline:`);
console.log(`   $ cdk deploy -c environment=${environment}`);
console.log(`\n   Destroy pipeline:`);
console.log(`   $ cdk destroy -c environment=${environment}`);

console.log(`\n   Important Notes:`);
if (environment === 'test') {
  console.log(`   - This pipeline will deploy infrastructure to TEST environment`);
  console.log(`   - Triggered by pushes to 'test' branch`);
  console.log(`   - No manual approval required`);
} else {
  console.log(`   - This pipeline will deploy infrastructure to PRODUCTION environment`);
  console.log(`   - Triggered by pushes to 'prod' branch`);
  console.log(`   - Manual approval ${pipelineConfig.requireManualApproval ? 'REQUIRED' : 'NOT REQUIRED'} before deployment`);
}

console.log(`\n Next steps:`);
console.log(`   1. Ensure GitHub connection is set up in AWS Console:`);
console.log(`      AWS CodePipeline → Settings → Connections`);
console.log(`   2. Update the connectionArn in the config file`);
console.log(`   3. Review configuration: lib/configs/${environment}-pipeline.config.ts`);
console.log(`   4. Run: cdk diff -c environment=${environment}`);
console.log(`   5. Deploy: cdk deploy -c environment=${environment}`);
console.log(`\n   Once deployed, the pipeline will automatically:`);
console.log(`   - Monitor the '${pipelineConfig.github.branchName}' branch`);
console.log(`   - Trigger on commits via webhook`);
console.log(`   - Deploy both normalize and ETL stacks to ${environment.toUpperCase()}`);
