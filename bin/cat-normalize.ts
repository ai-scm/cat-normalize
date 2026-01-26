#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { CatNormalizeStack } from '../lib/stacks/cat-normalize-stack';
import { CatEtlStack } from '../lib/stacks/cat-etl-stack';

// Import configurations
import { testNormalizeConfig } from '../lib/configs/test-normalize.config';
import { prodNormalizeConfig } from '../lib/configs/prod-normalize.config';
import { testEtlConfig } from '../lib/configs/test-etl.config';
import { prodEtlConfig } from '../lib/configs/prod-etl.config';

const app = new cdk.App();

// ==================== ENVIRONMENT SELECTION ====================
// Get environment from context or environment variable
// Usage: cdk deploy -c environment=test
//    or: ENVIRONMENT=test cdk deploy
const environment = app.node.tryGetContext('environment') || process.env.ENVIRONMENT || 'test';

console.log(`🚀 Deploying to environment: ${environment.toUpperCase()}`);

// Validate environment
if (environment !== 'test' && environment !== 'prod') {
  throw new Error(`❌ Unknown environment: ${environment}. Use 'test' or 'prod'.`);
}

// ==================== SELECT CONFIGURATIONS ====================
const normalizeConfig = environment === 'prod' ? prodNormalizeConfig : testNormalizeConfig;
const etlConfig = environment === 'prod' ? prodEtlConfig : testEtlConfig;

console.log(`📦 Using ${environment} configurations`);

// ==================== COMMON PROPS ====================
const commonEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: normalizeConfig.awsRegion
};

// ==================== STACK 1: NORMALIZE STACK ====================
const normalizeStackName = `cat-${environment}-normalize-stack`;

console.log(`\n📊 Creating Normalize Stack: ${normalizeStackName}`);
console.log(`   - S3 Bucket: ${normalizeConfig.s3BucketName}`);
console.log(`   - Lambda: ${normalizeConfig.lambda.name}`);
console.log(`   - Schedule: ${normalizeConfig.schedule.enabled ? 'ENABLED' : 'DISABLED'}`);

const normalizeStack = new CatNormalizeStack(
  app,
  normalizeStackName,
  normalizeConfig,
  {
    env: commonEnv,
    stackName: normalizeStackName,
    description: `Normalize Stack for ${environment} environment - ETL Process 1 (DynamoDB -> S3 CSV)`,
    tags: {
      ...normalizeConfig.tags,
      DeploymentDate: new Date().toISOString().split('T')[0],
      StackType: 'normalize-etl1'
    }
  }
);

// ==================== STACK 2: ETL STACK ====================
const etlStackName = `cat-${environment}-etl2-stack`;

console.log(`\n📊 Creating ETL Stack: ${etlStackName}`);
console.log(`   - Data Bucket: ${etlConfig.dataBucketName}`);
console.log(`   - Glue Database: ${etlConfig.catalog.databaseName}`);
console.log(`   - Athena WorkGroup: ${etlConfig.athena.workGroupName}`);

const etlStack = new CatEtlStack(
  app,
  etlStackName,
  etlConfig,
  {
    env: commonEnv,
    stackName: etlStackName,
    description: `ETL Stack for ${environment} environment - ETL Process 2 (CSV -> Parquet + Athena)`,
    tags: {
      ...etlConfig.tags,
      DeploymentDate: new Date().toISOString().split('T')[0],
      StackType: 'etl2-glue'
    }
  }
);

// Note: ETL Stack references an existing S3 bucket from Normalize Stack
// If you're deploying both stacks together for the first time, deploy Normalize Stack first
// The bucket is imported by name, so no direct dependency is needed for independent deployments


// ==================== APPLY GLOBAL TAGS ====================
cdk.Tags.of(app).add('Environment', environment.toUpperCase());
cdk.Tags.of(app).add('ManagedBy', 'CDK');
cdk.Tags.of(app).add('Project', 'CAT-DATA-PLATFORM');

// ==================== SYNTHESIS ====================
app.synth();

console.log('\n✅ CDK app synthesized successfully');
console.log(`\n📦 Stacks created:`);
console.log(`   - ${normalizeStackName}`);
console.log(`   - ${etlStackName}`);

console.log(`\n💡 Deployment commands:`);
console.log(`\n   Deploy all stacks:`);
console.log(`   $ cdk deploy --all -c environment=${environment}`);
console.log(`\n   Deploy specific stack:`);
console.log(`   $ cdk deploy ${normalizeStackName} -c environment=${environment}`);
console.log(`   $ cdk deploy ${etlStackName} -c environment=${environment}`);

if (environment === 'test') {
  console.log(`\n⚠️  Note: Schedules are DISABLED in test environment (manual execution only)`);
} else {
  console.log(`\n✅ Note: Schedules are ENABLED in production environment (automatic execution)`);
}

console.log(`\n📚 Next steps:`);
console.log(`   1. Review configurations in lib/configs/${environment}-*.config.ts`);
console.log(`   2. Run: cdk diff ${normalizeStackName} -c environment=${environment}`);
console.log(`   3. Deploy: cdk deploy ${normalizeStackName} -c environment=${environment}`);