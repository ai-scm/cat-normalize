import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';

export interface FoundationConfig {
  environment: 'test' | 'prod';
  bucketName: string;
  enableVersioning: boolean;
  tags?: Record<string, string>;
}

/**
 * FOUNDATION STACK - S3 BUCKET ONLY
 * 
 * Repository: cat-foundation
 * Stack Name: cat-{env}-normalize-stack (IMMUTABLE for prod)
 * 
 * This stack contains ONLY the shared S3 bucket.
 * All services import this bucket from their separate repositories.
 * 
 * Consumers:
 * 1. Services Repo (Lambda + Glue) - cat-services
 * 2. Tokens Repo - cat-tokens
 */
export class CatFoundationStack extends cdk.Stack {
  public readonly bucket: s3.IBucket;

  constructor(scope: Construct, id: string, config: FoundationConfig, props?: cdk.StackProps) {
    super(scope, id, props);

    console.log(`Foundation Stack - ${config.environment.toUpperCase()}`);
    console.log(`Bucket: ${config.bucketName}`);
    console.log(`Stack: ${this.stackName}`);

    // =================================================================
    // SHARED S3 BUCKET
    // =================================================================
    
    const bucket = new s3.Bucket(this, 'SharedReportsBucket', {
      bucketName: config.bucketName,
      versioned: config.enableVersioning,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      eventBridgeEnabled: true,  // For Glue orchestration
      
      lifecycleRules: [
        {
          id: 'DeleteOldVersions',
          enabled: true,
          noncurrentVersionExpiration: cdk.Duration.days(30),
        },
        {
          id: 'TransitionToIA',
          enabled: config.environment === 'prod',
          transitions: config.environment === 'prod' ? [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(90),
            },
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: cdk.Duration.days(180),
            },
          ] : [],
        },
      ],
    });

    this.bucket = bucket;

    // =================================================================
    // TAGS
    // =================================================================
    
    const defaultTags = {
      'Name': config.bucketName,
      'BillingTag': 'FOUNDATION-STORAGE',
      'CostCenter': 'DATA-ANALYTICS',
      'Project': 'CAT-FOUNDATION',
      'Environment': config.environment.toUpperCase(),
      'Component': 'SHARED-INFRASTRUCTURE',
      'ManagedBy': 'CDK',
      'SharedResource': 'true',
      'Consumers': 'Services,Tokens',
    };

    Object.entries({ ...defaultTags, ...config.tags }).forEach(([key, value]) => {
      cdk.Tags.of(bucket).add(key, value);
    });

    // =================================================================
    // EXPORTS - FOR OTHER REPOSITORIES
    // =================================================================
    
    new cdk.CfnOutput(this, 'BucketName', {
      value: bucket.bucketName,
      description: 'Shared bucket name',
      exportName: `${config.environment}-ReportsBucketName`,
    });

    new cdk.CfnOutput(this, 'BucketArn', {
      value: bucket.bucketArn,
      description: 'Shared bucket ARN',
      exportName: `${config.environment}-ReportsBucketArn`,
    });

    new cdk.CfnOutput(this, 'BucketDomainName', {
      value: bucket.bucketDomainName,
      description: 'Bucket domain name',
      exportName: `${config.environment}-ReportsBucketDomainName`,
    });

    new cdk.CfnOutput(this, 'FolderStructure', {
      value: JSON.stringify({
        'reports/etl-process1/': 'CSV from Lambda',
        'reports/etl-process2/': 'Parquet from Glue',
        'tokens-analysis/': 'Token data',
        'athena/results/': 'Athena queries',
      }),
      description: 'Bucket folder structure',
    });

    console.log(`   ✅ Bucket exported for Services and Tokens repos`);
  }
}