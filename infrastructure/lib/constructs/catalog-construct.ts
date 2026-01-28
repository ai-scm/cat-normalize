import { Construct } from "constructs";
import {
  aws_glue as glue,
  aws_iam as iam,
  aws_s3 as s3,
  aws_events as events,
  aws_events_targets as targets,
  CfnOutput,
} from "aws-cdk-lib";
import * as cdk from 'aws-cdk-lib';

export interface CatalogProps {
  /** Environment (test/prod) for resource naming */
  environment: string;
  dataBucket: s3.IBucket;
  curatedPrefix: string;   // "curated/"
  databaseName: string;    // "analytics_db"
  crawlerName?: string;    // Optional: override default naming
  glueJobName: string;     // Nombre del Glue Job ETL-2 para trigger
}

export class CatalogConstruct extends Construct {
  public readonly databaseName: string;
  public readonly crawlerName: string;

  constructor(scope: Construct, id: string, props: CatalogProps) {
    super(scope, id);

    const {
      environment,
      dataBucket,
      curatedPrefix,
      databaseName,
      crawlerName, // Will be overridden below
      glueJobName,
    } = props;

    // ✅ Dynamic crawler name based on environment
    const dynamicCrawlerName = crawlerName || `cat-${environment}-curated-crawler`;

    // Database L1
    const accountId = cdk.Stack.of(this).account || process.env.CDK_DEFAULT_ACCOUNT || '000000000000';
    const db = new glue.CfnDatabase(this, "AnalyticsDb", {
      catalogId: accountId,
      databaseInput: { name: databaseName },
    });

    // ✅ Role del Crawler con nombre dinámico
    const crawlerRole = new iam.Role(this, "CrawlerRole", {
      roleName: `${environment}-glue-crawler-role`,
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      description: `Role for Glue Crawler (${environment}) to read curated/ parquet`,
    });
    crawlerRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"),
    );
    dataBucket.grantRead(crawlerRole, `${curatedPrefix}*`);

    // ✅ Crawler sobre curated/ con nombre dinámico
    const crawler = new glue.CfnCrawler(this, "CuratedCrawler", {
      name: dynamicCrawlerName, // ✅ Dynamic name
      role: crawlerRole.roleArn,
      databaseName: databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${dataBucket.bucketName}/${curatedPrefix}data.parquet/`,
            // 🎯 Apunta específicamente a la carpeta del Parquet, no a scripts
          },
        ],
      },
      schemaChangePolicy: {
        deleteBehavior: "LOG",
        updateBehavior: "UPDATE_IN_DATABASE",
      },
      // 🚫 Sin schedule - se ejecuta por eventos
    }).addDependency(db);

    // 🎯 EventBridge Rule: Trigger crawler cuando ETL-2 Job completa exitosamente
    const crawlerTriggerRule = new events.Rule(this, "CrawlerTriggerRule", {
      ruleName: `${environment}-cat-crawler-trigger`, // ✅ Dynamic name
      description: `Trigger crawler when ETL-2 Glue Job completes successfully (${environment})`,
      eventPattern: {
        source: ["aws.glue"],
        detailType: ["Glue Job State Change"],
        detail: {
          jobName: [glueJobName],
          state: ["SUCCEEDED"]
        }
      }
    });

    // 🎯 Target: Ejecutar crawler
    crawlerTriggerRule.addTarget(new targets.AwsApi({
      service: "Glue",
      action: "startCrawler",
      parameters: {
        Name: dynamicCrawlerName
      },
      policyStatement: new iam.PolicyStatement({
        actions: ["glue:StartCrawler"],
        resources: [`arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:crawler/${dynamicCrawlerName}`]
      })
    }));

    this.databaseName = databaseName;
    this.crawlerName = dynamicCrawlerName;

    // 📊 Outputs informativos
    new CfnOutput(this, "GlueDatabaseName", { value: databaseName });
    new CfnOutput(this, "CrawlerName", { value: dynamicCrawlerName });
    new CfnOutput(this, "CrawlerTriggerInfo", { 
      value: `Crawler se ejecuta automáticamente cuando Job '${glueJobName}' completa exitosamente`,
      description: "Información del trigger automático del crawler"
    });
  }
}