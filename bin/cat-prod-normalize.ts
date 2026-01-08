#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { CatProdNormalizeStack } from '../lib/stacks/cat-prod-normalize-stack';
import { NewEtlStack } from '../lib/stacks/cad-prod-etl-stack';
import { CatProdTokensStack } from '../lib/stacks/cat-prod-tokens-stack';
import { testTokensConfig } from '../lib/configs/test-tokens.config';
import { prodTokensConfig } from '../lib/configs/prod-tokens.config';
import * as fs from 'fs';
import * as path from 'path';

const app = new cdk.App();

// ==================== LOAD CONFIGURATIONS ====================
const configPath = path.join(__dirname, '../config');
const accountConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'accountConfig.json'), 'utf8'));
const appConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'config.json'), 'utf8'));
const tagsConfig = JSON.parse(fs.readFileSync(path.join(configPath, 'tags.json'), 'utf8'));

// ==================== ENVIRONMENT SELECTION ====================
// Get environment from context or environment variable
// Usage: cdk deploy -c environment=test
//    or: ENVIRONMENT=test cdk deploy
const environment = app.node.tryGetContext('environment') || process.env.ENVIRONMENT || 'test';

console.log(`🚀 Deploying to environment: ${environment.toUpperCase()}`);

// Select tokens configuration based on environment
let tokensConfig;
switch (environment) {
  case 'test':
    tokensConfig = testTokensConfig;
    console.log('📦 Using test tokens configuration');
    break;
  case 'prod':
    tokensConfig = prodTokensConfig;
    console.log('🏭 Using production tokens configuration');
    break;
  default:
    throw new Error(`❌ Unknown environment: ${environment}. Use 'test' or 'prod'.`);
}

// Validate tokens configuration
if (!tokensConfig.oldDynamoTableName || !tokensConfig.newDynamoTableName || !tokensConfig.outputBucket) {
  throw new Error('❌ Invalid tokens configuration: Missing required fields');
}

// ==================== STACK PROPS INTERFACE ====================
interface StackProps extends cdk.StackProps {
  namespace: string;
  tags?: Record<string, string>;
}

// ==================== STACK 1: NORMALIZE STACK ====================
const normalizeStackName = `${appConfig.namespace}-normalize-stack`;
const normalizeStack = new CatProdNormalizeStack(app, normalizeStackName, {
  env: { 
    account: accountConfig.accountId, 
    region: accountConfig.region 
  },
  tags: tagsConfig,
  stackName: normalizeStackName,
  namespace: appConfig.namespace
} as StackProps);

// ==================== STACK 2: ETL-2 STACK ====================
const etl2StackName = `${appConfig.namespace}-etl2-stack`;
new NewEtlStack(app, etl2StackName, {
  env: { 
    account: accountConfig.accountId, 
    region: accountConfig.region 
  },
  dataBucketName: 'cat-prod-normalize-reports',
  cleanPrefix: 'reports/etl-process1/',
  curatedPrefix: 'reports/etl-process2/',
  athenaResultsPrefix: 'athena/results/',
  glueDatabaseName: 'cat_prod_analytics_db',
  athenaWorkGroup: 'wg-cat-prod-analytics'
});

// ==================== STACK 3: TOKENS STACK (UPDATED) ====================


const tokensStackName = `cat-${tokensConfig.environment}-tokens-stack`;

console.log(`📊 Creating Tokens Stack: ${tokensStackName}`);
console.log(`   - Archival Lambda: ${tokensConfig.archivalLambda.name}`);
console.log(`   - Consolidated Lambda: ${tokensConfig.consolidatedLambda.name}`);
console.log(`   - Schedule: ${tokensConfig.schedule.enabled ? 'ENABLED' : 'DISABLED'}`);

new CatProdTokensStack(
  app,
  tokensStackName,
  tokensConfig,
  {
    env: {
      account: accountConfig.accountId || process.env.CDK_DEFAULT_ACCOUNT,
      region: tokensConfig.awsRegion,
    },
    description: `Tokens Analysis Stack for ${environment} environment - Dual Lambda (Archival + Consolidated)`,
    tags: {
      ...tagsConfig,
      ...tokensConfig.tags,
      Environment: environment.toUpperCase(),
      StackType: 'tokens-processing',
      DeploymentDate: new Date().toISOString().split('T')[0]
    },
  }
);

// ==================== APPLY GLOBAL TAGS ====================
Object.keys(tagsConfig).forEach(key => {
  cdk.Tags.of(app).add(key, tagsConfig[key]);
});

// Add environment tag globally
cdk.Tags.of(app).add('DeploymentEnvironment', environment.toUpperCase());

// ==================== SYNTHESIS ====================
app.synth();

console.log('✅ CDK app synthesized successfully');
console.log(`📦 Stacks created:`);
console.log(`   - ${normalizeStackName}`);
console.log(`   - ${etl2StackName}`);
console.log(`   - ${tokensStackName}`);
console.log(`\n💡 Next steps:`);
console.log(`   1. Review configuration in lib/configs/${environment}-tokens.config.ts`);
console.log(`   2. Deploy: cdk deploy ${tokensStackName} -c environment=${environment}`);
console.log(`   3. Run archival Lambda once (one-time): aws lambda invoke --function-name ${tokensConfig.archivalLambda.name} --payload '{}' response.json`);
console.log(`   4. ${tokensConfig.schedule.enabled ? 'Consolidated Lambda will run daily automatically' : 'Run consolidated Lambda manually when needed'}`);