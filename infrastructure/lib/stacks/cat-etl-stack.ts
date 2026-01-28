import { Stack, StackProps, aws_s3 as s3, Tags, CfnOutput } from "aws-cdk-lib";
import { Construct } from "constructs";
import { TransformJobConstruct } from "../constructs/transform-job-construct";
import { CatalogConstruct } from "../constructs/catalog-construct";
import { AthenaConstruct } from "../constructs/athena-construct";
import { OrchestratorConstruct } from "../constructs/orchestrator-construct";
import { EtlConfig } from '../configs/etl-config.interface';

export class CatEtlStack extends Stack {
  public readonly dataBucket: s3.IBucket;
  public readonly transformJob: TransformJobConstruct;

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

    // 1) Glue Job ETL-2: Lee CSV de etl-process1/ y escribe Parquet único en etl-process2/
    this.transformJob = new TransformJobConstruct(this, "TransformJob", {
      environment: config.environment, // ✅ Pass environment first
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
      environment: config.environment, // ✅ Pass environment first
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
      environment: config.environment, // ✅ Pass environment first
      dataBucket: this.dataBucket,
      cleanPrefix: config.cleanPrefix,
      glueJobName: this.transformJob.jobName,
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
        throw new Error(`❌ Missing required configuration: ${field}`);
      }
    }
  }

  /**
   * Apply tags specifically to Glue Job
   */
  private applyGlueJobTags(config: EtlConfig): void {
    Tags.of(this.transformJob).add('ETLComponent', 'ETL-2');
    Tags.of(this.transformJob).add('ETLStage', 'TRANSFORM-LOAD');
    Tags.of(this.transformJob).add('DataSource', 'S3-CSV');
    Tags.of(this.transformJob).add('DataTarget', 'S3-PARQUET');
    Tags.of(this.transformJob).add('ResourceType', 'COMPUTE-GLUE');
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
  }
}