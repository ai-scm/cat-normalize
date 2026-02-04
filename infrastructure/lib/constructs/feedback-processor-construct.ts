import { Construct } from 'constructs';
import { Duration, aws_lambda as lambda, aws_iam as iam, aws_s3 as s3, aws_events as events, aws_events_targets as targets } from 'aws-cdk-lib';

export interface FeedbackProcessorConstructProps {
  /**
   * Environment (dev, staging, prod)
   */
  region: string;
  environment: string;

  /**
   * DynamoDB table name for conversations
   */
  dynamoDbTableName: string;

  /**
   * S3 bucket for output reports
   */
  outputBucket: s3.IBucket;

  /**
   * S3 prefix for feedback reports
   */
  outputPrefix: string;

  /**
   * Lambda memory size in MB
   * @default 512
   */
  memorySize?: number;

  /**
   * Lambda timeout in minutes
   * @default 5
   */
  timeoutMinutes?: number;

  /**
   * Schedule expression for automatic execution (optional)
   * Example: 'cron(0 6 * * ? *)' for daily at 6 AM UTC
   */
  scheduleExpression?: string,
  layerArn: string;
}

export class FeedbackProcessorConstruct extends Construct {
  public readonly lambdaFunction: lambda.Function;
  public readonly functionName: string;

  constructor(scope: Construct, id: string, props: FeedbackProcessorConstructProps) {
    super(scope, id);

    // Create Lambda execution role with necessary permissions
    const {
      region,
      environment,
      dynamoDbTableName,
      outputBucket,
      outputPrefix,
      memorySize,
      timeoutMinutes,
      scheduleExpression,
      layerArn,
    } = props;

    const pythonLayer = lambda.LayerVersion.fromLayerVersionArn(
  this,
  'AWSSDKPandasLayer',
  layerArn
);

    const lambdaRole = new iam.Role(this, 'FeedbackProcessorRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      description: 'Execution role for Feedback Processor Lambda',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });

    // Grant DynamoDB scan permissions
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['dynamodb:Scan'],
        resources: [
          `arn:aws:dynamodb:${region}:*:table/${dynamoDbTableName}`,
        ],
      })
    );

    // Grant S3 write permissions to output bucket
    lambdaRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['s3:PutObject', 's3:PutObjectAcl'],
        resources: [
          `${outputBucket.bucketArn}/${outputPrefix}*`,
        ],
      })
    );
    props.outputBucket.grantPut(lambdaRole);

    // Create Lambda function
    this.lambdaFunction = new lambda.Function(this, 'FeedbackProcessorFunction', {
      functionName: `cat-${environment}-feedback-processor`,
      runtime: lambda.Runtime.PYTHON_3_11,
      layers: [pythonLayer],
      handler: 'lambda_feedback_processor.lambda_handler',
      code: lambda.Code.fromAsset('lambda/feedback-processor', {
        exclude: ['*.pyc', '__pycache__','test_local.py'],
      }),
      role: lambdaRole,
      memorySize: memorySize || 512,
      timeout: Duration.minutes(timeoutMinutes || 5),
      environment: {
        DYNAMODB_TABLE_NAME: dynamoDbTableName,
        S3_BUCKET_NAME: outputBucket.bucketName,
        S3_OUTPUT_PREFIX:outputPrefix,
      },
      description: 'Processes DynamoDB chatbot feedback and generates analysis reports',
    });

    this.functionName = this.lambdaFunction.functionName;

    // Create EventBridge rule for scheduled execution (if provided)
    if (props.scheduleExpression) {
      const rule = new events.Rule(this, 'ScheduleRule', {
        ruleName: `cat-${props.environment}-feedback-processor-schedule`,
        description: 'Trigger feedback processor on schedule',
        schedule: events.Schedule.expression(props.scheduleExpression),
      });

      rule.addTarget(new targets.LambdaFunction(this.lambdaFunction));
    }
  }
}