import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { NormalizeConfig } from '../configs/normalize-config.interface';

export class CatNormalizeStack extends cdk.Stack {
  public readonly bucket: s3.Bucket;
  public readonly etlLambda: lambda.Function;

  constructor(
    scope: Construct,
    id: string,
    config: NormalizeConfig,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    // Validate configuration
    this.validateConfig(config);

    // 📦 S3 Bucket para reportes
    this.bucket = this.createS3Bucket(config);

    // 🔐 IAM Roles
    const etlLambdaRole = this.createETLLambdaRole(this.bucket, config);

    // 📦 Lambda Layer for Python dependencies
    const pythonLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      'AWSSDKPandasLayer',
      config.lambda.layerArn
    );

    // ⚡ Lambda ETL
    this.etlLambda = this.createETLLambda(etlLambdaRole, this.bucket, config, pythonLayer);

    // 🕐 EventBridge Schedule (only if enabled)
    if (config.schedule.enabled) {
      this.createEventBridgeSchedule(this.etlLambda, config);
    }

    // 🏷️ Apply tags to all resources
    this.applyTags(config);

    // 📤 Stack Outputs
    this.createOutputs(config);
  }

  /**
   * Validate configuration
   */
  private validateConfig(config: NormalizeConfig): void {
    const required = [
      's3BucketName',
      'dynamoTableName',
      'lambda',
      'schedule'
    ];

    for (const field of required) {
      if (!config[field as keyof NormalizeConfig]) {
        throw new Error(`❌ Missing required configuration: ${field}`);
      }
    }
  }

  /**
   * Create S3 Bucket
   */
  private createS3Bucket(config: NormalizeConfig): s3.Bucket {
    const bucket = new s3.Bucket(this, 'NormalizeReportsBucket', {
      bucketName: config.s3BucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      eventBridgeEnabled: true,
      lifecycleRules: config.lifecycle?.enabled ? [
        {
          id: 'DeleteOldVersions',
          enabled: true,
          noncurrentVersionExpiration: cdk.Duration.days(
            config.lifecycle.noncurrentVersionExpiration
          )
        }
      ] : []
    });

    // Apply bucket-specific tags
    cdk.Tags.of(bucket).add('ResourceType', 'STORAGE');
    cdk.Tags.of(bucket).add('ETLComponent', 'DATA-LAKE');

    return bucket;
  }

  /**
   * Create IAM Role for Lambda ETL
   */
  private createETLLambdaRole(bucket: s3.Bucket, config: NormalizeConfig): iam.Role {
    return new iam.Role(this, 'ETLLambdaRole', {
      roleName: `${config.environment}-normalize-etl-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:GetItem', 'dynamodb:Query', 'dynamodb:Scan'],
              resources: [`arn:aws:dynamodb:*:*:table/${config.dynamoTableName}`],
            }),
          ],
        }),
        S3Access: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:PutObjectAcl'],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
            }),
          ],
        }),
      },
    });
  }

  /**
   * Create Lambda ETL Function
   */
  private createETLLambda(
    role: iam.Role,
    bucket: s3.Bucket,
    config: NormalizeConfig,
    pythonLayer: lambda.ILayerVersion
  ): lambda.Function {
    const lambdaFunction = new lambda.Function(this, 'NormalizeLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      functionName: config.lambda.name,
      handler: config.lambda.handler,
      code: lambda.Code.fromAsset(config.lambda.codePath, {
        exclude: ['requirements.txt', '*.pyc', '__pycache__']
      }),
      layers: [pythonLayer],
      role: role,
      timeout: cdk.Duration.seconds(config.lambda.timeout),
      memorySize: config.lambda.memorySize,
      environment: {
        S3_BUCKET_NAME: bucket.bucketName,
        DYNAMODB_TABLE_NAME: config.dynamoTableName,
        PROJECT_ID: config.environmentVariables.projectId,
        ENVIRONMENT: config.environment.toUpperCase(),
        CLIENT: config.environmentVariables.client,
        ...config.environmentVariables
      },
      description: `Lambda function for normalizing conversation data - ${config.environment} environment`
    });

    // Apply Lambda-specific tags
    cdk.Tags.of(lambdaFunction).add('Component', 'lambda-function');
    cdk.Tags.of(lambdaFunction).add('Purpose', 'data-processing');
    cdk.Tags.of(lambdaFunction).add('Runtime', config.lambda.runtime);
    cdk.Tags.of(lambdaFunction).add('ETLComponent', 'ETL-1');
    cdk.Tags.of(lambdaFunction).add('ETLStage', 'EXTRACT-TRANSFORM');
    cdk.Tags.of(lambdaFunction).add('DataSource', 'DynamoDB');
    cdk.Tags.of(lambdaFunction).add('DataTarget', 'S3-CSV');
    cdk.Tags.of(lambdaFunction).add('ResourceType', 'COMPUTE');

    return lambdaFunction;
  }

  /**
   * Create EventBridge schedule for daily execution
   */
  private createEventBridgeSchedule(
    lambdaFunction: lambda.Function,
    config: NormalizeConfig
  ): void {
    const rule = new events.Rule(this, 'DailyETLSchedule', {
      ruleName: `${config.environment}-cat-daily-etl-schedule`,
      description: config.schedule.description,
      schedule: events.Schedule.expression(config.schedule.cronExpression),
      enabled: config.schedule.enabled
    });

    rule.addTarget(new targets.LambdaFunction(lambdaFunction, {
      maxEventAge: cdk.Duration.hours(2),
      retryAttempts: 2
    }));

    // Apply schedule-specific tags
    cdk.Tags.of(rule).add('ResourceType', 'EVENT-ORCHESTRATION');
    cdk.Tags.of(rule).add('ETLComponent', 'ETL-1-SCHEDULER');
    cdk.Tags.of(rule).add('ScheduleType', 'daily');
  }

  /**
   * Apply tags to stack and resources
   */
  private applyTags(config: NormalizeConfig): void {
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(this).add(key, value);
      });
    }
  }

  /**
   * Create stack outputs
   */
  private createOutputs(config: NormalizeConfig): void {
    new cdk.CfnOutput(this, 'Environment', {
      value: config.environment,
      description: 'Deployment environment'
    });

    new cdk.CfnOutput(this, 'LambdaFunctionName', {
      value: this.etlLambda.functionName,
      description: 'Lambda ETL function name'
    });

    new cdk.CfnOutput(this, 'S3BucketName', {
      value: this.bucket.bucketName,
      description: 'S3 bucket for reports'
    });

    new cdk.CfnOutput(this, 'PythonLayerArn', {
      value: config.lambda.layerArn,
      description: 'ARN of Python dependencies layer'
    });

    if (config.schedule.enabled) {
      new cdk.CfnOutput(this, 'ScheduleStatus', {
        value: `Enabled - ${config.schedule.description}`,
        description: 'EventBridge schedule status'
      });
    } else {
      new cdk.CfnOutput(this, 'ScheduleStatus', {
        value: 'Disabled - Manual execution only',
        description: 'EventBridge schedule status'
      });
    }

    new cdk.CfnOutput(this, 'DynamoDBTable', {
      value: config.dynamoTableName,
      description: 'DynamoDB table for conversation data'
    });
  }
}