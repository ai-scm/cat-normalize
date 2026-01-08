import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as path from 'path';
import { TokensConfig } from '../configs/tokens-config.interface';

export class CatProdTokensStack extends cdk.Stack {
  constructor(
    scope: Construct,
    id: string,
    config: TokensConfig,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    // Validate configuration
    this.validateConfig(config);

    // 📦 Get S3 bucket reference
    const reportsBucket = s3.Bucket.fromBucketName(
      this,
      'TokensReportsBucket',
      config.outputBucket
    );

    // 🔧 Create Lambda Layer for Python dependencies
    const pythonLayer = this.createPythonLayer(config);

    // ⚡ Lambda 1: Archival Processing (one-time execution for old table)
    const archivalLambda = this.createArchivalLambda(
      reportsBucket,
      config,
      pythonLayer
    );

    // ⚡ Lambda 2: Consolidated Processing (daily execution for new table)
    const consolidatedLambda = this.createConsolidatedLambda(
      reportsBucket,
      config,
      pythonLayer
    );

    // 🕐 EventBridge Schedule (only if enabled)
    if (config.schedule.enabled) {
      this.createEventBridgeSchedule(consolidatedLambda, config);
    }

    // 🏷️ Apply tags to all resources
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(this).add(key, value);
      });
    }

    // 📤 Stack Outputs
    this.createOutputs(archivalLambda, consolidatedLambda, config, pythonLayer);
  }

  /**
   * Validate configuration
   */
  private validateConfig(config: TokensConfig): void {
    const required = [
      'oldDynamoTableName',
      'newDynamoTableName',
      'outputBucket',
      'athenaDatabase'
    ];

    for (const field of required) {
      if (!config[field as keyof TokensConfig]) {
        throw new Error(`❌ Missing required configuration: ${field}`);
      }
    }
  }

  /**
   * Create Python dependencies layer
   */
  private createPythonLayer(config: TokensConfig): lambda.LayerVersion {
    return new lambda.LayerVersion(this, 'PythonDependenciesLayer', {
      layerVersionName: config.layerName,
      code: lambda.Code.fromAsset(path.join(__dirname, '../../lambda/tokens-process'), {
        bundling: {
          image: lambda.Runtime.PYTHON_3_11.bundlingImage,
          command: [
            'bash', '-c', [
              'pip install --platform manylinux2014_x86_64 --target /asset-output/python --implementation cp --python-version 3.11 --only-binary=:all: --upgrade pandas',
              'echo "✅ Layer created successfully for tokens processing"'
            ].join(' && ')
          ],
          user: 'root'
        }
      }),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
      description: `[${config.environment.toUpperCase()}] Pandas, boto3, numpy for token analysis`
    });
  }

  /**
   * Create Archival Lambda (processes old table - one-time)
   */
  private createArchivalLambda(
    bucket: s3.IBucket,
    config: TokensConfig,
    pythonLayer: lambda.LayerVersion
  ): lambda.Function {
    // Create IAM role
    const role = new iam.Role(this, 'ArchivalLambdaRole', {
      roleName: `${config.environment}-tokens-archival-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBReadAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Scan', 'dynamodb:GetItem'],
              resources: [`arn:aws:dynamodb:*:*:table/${config.oldDynamoTableName}`],
            }),
          ],
        }),
        S3WriteAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:PutObject', 's3:PutObjectAcl'],
              resources: [`${bucket.bucketArn}/${config.historicalPrefix}*`],
            }),
          ],
        }),
      },
    });

    // Create Lambda function
    const lambdaFunction = new lambda.Function(this, 'ArchivalLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      functionName: config.archivalLambda.name,
      handler: config.archivalLambda.handler,
      code: lambda.Code.fromAsset(path.join(__dirname, '../../lambda/tokens-process'), {
        exclude: ['requirements.txt', '*.pyc', '__pycache__', 'tokens_lambda.py']
      }),
      layers: [pythonLayer],
      role: role,
      timeout: cdk.Duration.seconds(config.archivalLambda.timeout),
      memorySize: config.archivalLambda.memorySize,
      environment: {
        OLD_DYNAMODB_TABLE_NAME: config.oldDynamoTableName,
        S3_BUCKET_NAME: config.outputBucket,
        S3_OLD_DATA_PREFIX: config.historicalPrefix,
        FILTER_DATE_START: config.dateFilter.archivalStart,
        FILTER_DATE_END: config.dateFilter.archivalEnd,
        ENVIRONMENT: config.environment.toUpperCase()
      },
      description: `[${config.environment.toUpperCase()}] Archival processing - one-time execution for historical token analysis`
    });

    // Apply tags
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(lambdaFunction).add(key, value);
      });
    }

    // Additional tags specific to this Lambda
    cdk.Tags.of(lambdaFunction).add('LambdaType', 'archival-processing');
    cdk.Tags.of(lambdaFunction).add('ExecutionFrequency', 'one-time');
    cdk.Tags.of(lambdaFunction).add('DataSource', config.oldDynamoTableName);

    return lambdaFunction;
  }

  /**
   * Create Consolidated Lambda (processes new table + consolidates - daily)
   */
  private createConsolidatedLambda(
    bucket: s3.IBucket,
    config: TokensConfig,
    pythonLayer: lambda.LayerVersion
  ): lambda.Function {
    // Create IAM role
    const role = new iam.Role(this, 'ConsolidatedLambdaRole', {
      roleName: `${config.environment}-tokens-consolidated-role`,
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: {
        DynamoDBReadAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['dynamodb:Scan', 'dynamodb:GetItem'],
              resources: [`arn:aws:dynamodb:*:*:table/${config.newDynamoTableName}`],
            }),
          ],
        }),
        S3FullAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['s3:GetObject', 's3:PutObject', 's3:ListBucket','s3:GetBucketLocation',],
              resources: [bucket.bucketArn, `${bucket.bucketArn}/*`],
            }),
          ],
        }),
        AthenaAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryExecution',
                'athena:GetQueryResults'
              ],
              resources: ['*'],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'glue:GetTable',
                'glue:GetPartitions',
                'glue:GetDatabase',
                'glue:CreateTable',
                'glue:UpdateTable'
              ],
              resources: [
                `arn:aws:glue:*:*:catalog`,
                `arn:aws:glue:*:*:database/${config.athenaDatabase}`,
                `arn:aws:glue:*:*:table/${config.athenaDatabase}/*`
              ]
            })
          ],
        }),
      },
    });

    // Create Lambda function
    const lambdaFunction = new lambda.Function(this, 'ConsolidatedLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      functionName: config.consolidatedLambda.name,
      handler: config.consolidatedLambda.handler,
      code: lambda.Code.fromAsset(path.join(__dirname, '../../lambda/tokens-process'), {
        exclude: ['requirements.txt', '*.pyc', '__pycache__', 'lambda-tokens-archival-processing.py']
      }),
      layers: [pythonLayer],
      role: role,
      timeout: cdk.Duration.seconds(config.consolidatedLambda.timeout),
      memorySize: config.consolidatedLambda.memorySize,
      environment: {
        DYNAMODB_TABLE_NAME: config.newDynamoTableName,
        S3_BUCKET_NAME: config.outputBucket,
        S3_OUTPUT_PREFIX: config.outputPrefix,
        S3_OLD_DATA_PREFIX: config.historicalPrefix,
        ATHENA_DATABASE: config.athenaDatabase,
        ATHENA_WORKGROUP: config.athenaWorkgroup,
        ATHENA_OUTPUT_LOCATION: config.athenaResultsLocation,
        ATHENA_VIEW_NAME: config.athenaViewName,
        FILTER_DATE_START: config.dateFilter.consolidatedStart,
        ENVIRONMENT: config.environment.toUpperCase(),
        PROJECT_ID: 'P2124',
        CLIENT: 'CAT'
      },
      description: `[${config.environment.toUpperCase()}] Daily processing of new table and consolidation with historical data`
    });

    // Apply tags
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(lambdaFunction).add(key, value);
      });
    }

    // Additional tags specific to this Lambda
    cdk.Tags.of(lambdaFunction).add('LambdaType', 'consolidated-processing');
    cdk.Tags.of(lambdaFunction).add('ExecutionFrequency', 'daily');
    cdk.Tags.of(lambdaFunction).add('DataSource', config.newDynamoTableName);

    return lambdaFunction;
  }

  /**
   * Create EventBridge schedule for daily execution
   */
  private createEventBridgeSchedule(
    lambdaFunction: lambda.Function,
    config: TokensConfig
  ): void {
    const rule = new events.Rule(this, 'DailyTokensSchedule', {
      ruleName: `${config.environment}-cat-daily-tokens-schedule`,
      description: config.schedule.description,
      schedule: events.Schedule.expression(config.schedule.cronExpression),
      enabled: config.schedule.enabled
    });

    rule.addTarget(new targets.LambdaFunction(lambdaFunction, {
      maxEventAge: cdk.Duration.hours(2),
      retryAttempts: 2
    }));

    // Apply tags
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(rule).add(key, value);
      });
    }

    cdk.Tags.of(rule).add('ResourceType', 'EVENT-ORCHESTRATION');
    cdk.Tags.of(rule).add('ScheduleType', 'daily');
  }

  /**
   * Create stack outputs
   */
  private createOutputs(
    archivalLambda: lambda.Function,
    consolidatedLambda: lambda.Function,
    config: TokensConfig,
    pythonLayer: lambda.LayerVersion
  ): void {
    new cdk.CfnOutput(this, 'Environment', {
      value: config.environment,
      description: 'Deployment environment'
    });

    new cdk.CfnOutput(this, 'ArchivalLambdaName', {
      value: archivalLambda.functionName,
      description: 'Archival Lambda function name (run once for historical data)'
    });

    new cdk.CfnOutput(this, 'ConsolidatedLambdaName', {
      value: consolidatedLambda.functionName,
      description: 'Consolidated Lambda function name (runs daily)'
    });

    new cdk.CfnOutput(this, 'PythonLayerArn', {
      value: pythonLayer.layerVersionArn,
      description: 'ARN of Python dependencies layer'
    });

    new cdk.CfnOutput(this, 'S3OutputBucket', {
      value: config.outputBucket,
      description: 'S3 bucket for token analysis outputs'
    });

    new cdk.CfnOutput(this, 'HistoricalDataPath', {
      value: `s3://${config.outputBucket}/${config.historicalPrefix}`,
      description: 'S3 path for historical token data'
    });

    new cdk.CfnOutput(this, 'ConsolidatedDataPath', {
      value: `s3://${config.outputBucket}/${config.outputPrefix}`,
      description: 'S3 path for consolidated token data'
    });

    new cdk.CfnOutput(this, 'ArchivalExecutionCommand', {
      value: `aws lambda invoke --function-name ${archivalLambda.functionName} --payload '{}' response.json`,
      description: 'Command to execute archival processing (run once)'
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

    new cdk.CfnOutput(this, 'AthenaDatabase', {
      value: config.athenaDatabase,
      description: 'Athena database for token analysis queries'
    });
  }
}