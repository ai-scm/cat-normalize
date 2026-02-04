import { Construct } from "constructs";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Tags } from "aws-cdk-lib";

export interface FeedbackCatalogConstructProps {
  /**
   * Deployment environment (e.g., dev, prod)
   */
  environment: string;

  /**
   * S3 bucket where feedback reports are stored
   */
  dataBucket: s3.IBucket;

  /**
   * S3 prefix where feedback Parquet files are located
   */
  feedbackPrefix: string;

  /**
   * Existing Glue database name for feedback catalog
   */
  databaseName: string;

  /**
   * Optional: Lambda function name to track dependency
   */
  lambdaFunctionName?: string;
}

/**
 * Construct that creates Glue Catalog resources for feedback analysis data
 * 
 * Creates:
 * - Glue Crawler to scan Parquet files and create/update table schema
 * - IAM role with necessary permissions
 * 
 * Note: Uses an existing Glue Database (does not create it)
 */
export class FeedbackCatalogConstruct extends Construct {
  public readonly databaseName: string;
  public readonly crawler: glue.CfnCrawler;
  public readonly crawlerRole: iam.Role;
  public readonly tableName: string;

  constructor(scope: Construct, id: string, props: FeedbackCatalogConstructProps) {
    super(scope, id);

    // Reference existing Glue Database
    this.databaseName = props.databaseName;

    // IAM Role for Crawler
    this.crawlerRole = new iam.Role(this, "FeedbackCrawlerRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      description: "Role for Glue Crawler to catalog feedback data",
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
      ],
    });

    // Grant S3 read permissions to the crawler
    props.dataBucket.grantRead(this.crawlerRole, `${props.feedbackPrefix}*`);

    // Expected table name (Glue creates this automatically based on S3 structure)
    this.tableName = `${props.environment}_feedback_analysis`;

    // Glue Crawler
    this.crawler = new glue.CfnCrawler(this, "FeedbackCrawler", {
      name: `${props.environment}-feedback-catalog-crawler`,
      role: this.crawlerRole.roleArn,
      databaseName: this.databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${props.dataBucket.bucketName}/${props.feedbackPrefix}`,
          },
        ],
      },
      schemaChangePolicy: {
        updateBehavior: "UPDATE_IN_DATABASE",
        deleteBehavior: "LOG",
      },
      configuration: JSON.stringify({
        Version: 1.0,
        CrawlerOutput: {
          Partitions: { AddOrUpdateBehavior: "InheritFromTable" },
        },
        Grouping: {
          TableGroupingPolicy: "CombineCompatibleSchemas",
        },
      }),
      description: `Crawler for feedback analysis Parquet files in ${props.environment}`,
    });

    // Apply tags
    this.applyTags(props);
  }

  /**
   * Apply tags to catalog resources
   */
  private applyTags(props: FeedbackCatalogConstructProps): void {
    Tags.of(this.crawler).add("Environment", props.environment);
    Tags.of(this.crawler).add("Component", "DataCatalog");
    Tags.of(this.crawler).add("DataSource", "S3-Parquet-Feedback");
    Tags.of(this.crawler).add("ResourceType", "CATALOG-CRAWLER");

    if (props.lambdaFunctionName) {
      Tags.of(this.crawler).add("DataProducer", props.lambdaFunctionName);
    }
  }
}