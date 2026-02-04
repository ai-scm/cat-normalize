import { Stack, StackProps, aws_s3 as s3, Tags, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import { TransformJobConstruct } from "../constructs/transform-job-construct";
import { CatalogConstruct } from "../constructs/catalog-construct";
import { AthenaConstruct } from "../constructs/athena-construct";
import { OrchestratorConstruct } from "../constructs/orchestrator-construct";
import { FeedbackProcessorConstruct } from "../constructs/feedback-processor-construct";
import { FeedbackCatalogConstruct } from "../constructs/feedback-catalog-construct";
import { EtlConfig } from '../configs/etl-config.interface';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export class CatEtlStack extends Stack {
  public readonly dataBucket: s3.IBucket;
  public readonly transformJob: TransformJobConstruct;
  public readonly feedbackProcessor: FeedbackProcessorConstruct;
  public readonly feedbackCatalog: FeedbackCatalogConstruct;

  constructor(
    scope: Construct,
    id: string,
    config: EtlConfig,
    props?: StackProps
  ) {
    super(scope, id, props);

    // Validate configuration
    this.validateConfig(config);

    // Importa el bucket existente (no lo recrea)
    this.dataBucket = s3.Bucket.fromBucketName(
      this,
      "DataLakeBucket",
      config.dataBucketName,
    );

    const pythonLayer = lambda.LayerVersion.fromLayerVersionArn(
          this,
          'AWSSDKPandasLayer',
          config.lambda.layerArn
        );

    // 1) Glue Job ETL-2: Lee CSV de etl-process1/ y escribe Parquet único en etl-process2/
    this.transformJob = new TransformJobConstruct(this, "TransformJob", {
      environment: config.environment,
      dataBucket: this.dataBucket,
      inputPrefix: config.cleanPrefix,
      outputPrefix: config.curatedPrefix,
      scriptS3Uri: config.glue.scriptS3Uri,
      glueVersion: config.glue.version,
      numberOfWorkers: config.glue.numberOfWorkers,
      workerType: config.glue.workerType as "G.1X" | "G.2X" | "G.4X",
    });

    // Apply tags to Glue Job
    this.applyGlueJobTags(config);

    // 2) Catálogo + Crawler: Escanea etl-process2/ para detectar esquema del archivo Parquet único
    new CatalogConstruct(this, "Catalog", {
      environment: config.environment,
      dataBucket: this.dataBucket,
      curatedPrefix: config.curatedPrefix,
      databaseName: config.catalog.databaseName,
      glueJobName: this.transformJob.jobName,
    });

    // 3) Athena WorkGroup
    new AthenaConstruct(this, "Athena", {
      dataBucket: this.dataBucket,
      resultsPrefix: config.athenaResultsPrefix,
      workGroupName: config.athena.workGroupName,
    });

    // 4) Orquestación: Detecta archivos nuevos en etl-process1/ -> Dispara Glue Job
    new OrchestratorConstruct(this, "Orchestrator", {
      environment: config.environment,
      dataBucket: this.dataBucket,
      cleanPrefix: config.cleanPrefix,
      glueJobName: this.transformJob.jobName,
    });

    // 5) Feedback Processor Lambda: Procesa feedbacks de DynamoDB y genera reportes en S3
    this.feedbackProcessor = new FeedbackProcessorConstruct(this, "FeedbackProcessor", {
      region : config.awsRegion,
      environment: config.environment,
      dynamoDbTableName: config.lambda.dynamoDbTableName,
      outputBucket: this.dataBucket,
      outputPrefix: config.lambda.outputPrefix,
      memorySize: config.lambda.memorySize,
      timeoutMinutes: config.lambda.timeoutMinutes,
      scheduleExpression: config.lambda.scheduleExpression,
    });

    // 6) Feedback Catalog: Crawler para catalogar reportes de feedback en Athena
    this.feedbackCatalog = new FeedbackCatalogConstruct(this, "FeedbackCatalog", {
      environment: config.environment,
      dataBucket: this.dataBucket,
      feedbackPrefix: config.lambda.outputPrefix,
      databaseName: config.catalog.databaseName, // Usa la misma base de datos existente
      lambdaFunctionName: this.feedbackProcessor.functionName,
    });

    // Apply tags to all stack resources
    this.applyTags(config);

    // Create outputs
    this.createOutputs(config);
  }

  /**
   * Validate configuration
   */
  private validateConfig(config: EtlConfig): void {
    const required = [
      'dataBucketName',
      'cleanPrefix',
      'curatedPrefix',
      'athenaResultsPrefix'
    ];

    for (const field of required) {
      if (!config[field as keyof EtlConfig]) {
        throw new Error(`Missing required configuration: ${field}`);
      }
    }

    // Validate feedback configuration if present
    if (config.lambda) {
      if (!config.lambda.dynamoDbTableName) {
        throw new Error('Missing required configuration: feedback.dynamoDbTableName');
      }
      if (!config.lambda.outputPrefix) {
        throw new Error('Missing required configuration: lambda.outputPrefix');
      }
    }
  }

  /**
   * Apply tags specifically to Glue Job
   */
  private applyGlueJobTags(config: EtlConfig): void {
    Tags.of(this.transformJob).add('ETLComponent', 'ETL-2');
    Tags.of(this.transformJob).add('ProjectId', 'P2124');
    Tags.of(this.transformJob).add('Environment', config.environment);
  }

  /**
   * Apply tags to stack and resources
   */
  private applyTags(config: EtlConfig): void {
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        Tags.of(this).add(key, value);
      });
    }
  }

  /**
   * Create stack outputs
   */
  private createOutputs(config: EtlConfig): void {
    new CfnOutput(this, 'Environment', {
      value: config.environment,
      description: 'Deployment environment'
    });

    new CfnOutput(this, 'DataBucket', {
      value: config.dataBucketName,
      description: 'S3 bucket for data lake'
    });

    new CfnOutput(this, 'GlueJobName', {
      value: this.transformJob.jobName,
      description: 'Glue Job name for ETL-2 processing'
    });

    new CfnOutput(this, 'GlueDatabase', {
      value: config.catalog.databaseName,
      description: 'Glue Catalog database name'
    });

    new CfnOutput(this, 'AthenaWorkGroup', {
      value: config.athena.workGroupName,
      description: 'Athena WorkGroup for queries'
    });

    new CfnOutput(this, 'CleanDataPath', {
      value: `s3://${config.dataBucketName}/${config.cleanPrefix}`,
      description: 'S3 path for clean data (CSV from ETL-1)'
    });

    new CfnOutput(this, 'CuratedDataPath', {
      value: `s3://${config.dataBucketName}/${config.curatedPrefix}`,
      description: 'S3 path for curated data (Parquet from ETL-2)'
    });

    // Feedback Processor outputs
    if (this.feedbackProcessor) {
      new CfnOutput(this, 'FeedbackProcessorFunctionName', {
        value: this.feedbackProcessor.functionName,
        description: 'Lambda function name for feedback processing'
      });

      new CfnOutput(this, 'FeedbackProcessorFunctionArn', {
        value: this.feedbackProcessor.lambdaFunction.functionArn,
        description: 'Lambda function ARN for feedback processing'
      });

      new CfnOutput(this, 'FeedbackOutputPath', {
        value: `s3://${config.dataBucketName}/${config.lambda.outputPrefix}`,
        description: 'S3 path for feedback analysis reports'
      });

      new CfnOutput(this, 'DynamoDBTableName', {
        value: config.lambda.dynamoDbTableName,
        description: 'DynamoDB table name for conversations'
      });
    }

    // Feedback Catalog outputs
    if (this.feedbackCatalog) {
      new CfnOutput(this, 'FeedbackCatalogDatabase', {
        value: this.feedbackCatalog.databaseName,
        description: 'Glue Catalog database for feedback data (existing)'
      });

      new CfnOutput(this, 'FeedbackCrawlerName', {
        value: this.feedbackCatalog.crawler.name || '',
        description: 'Glue Crawler name for feedback catalog'
      });

      new CfnOutput(this, 'FeedbackTableName', {
        value: this.feedbackCatalog.tableName,
        description: 'Expected Glue table name for feedback data (created by crawler)'
      });
    }
  }
}