import { Construct } from "constructs";
import { aws_events as events, aws_events_targets as targets, aws_iam as iam, aws_s3 as s3 } from "aws-cdk-lib";

export interface OrchestratorProps {
  /** Environment (test/prod) for resource naming */
  environment: string;
  dataBucket: s3.IBucket;
  cleanPrefix: string; // "clean/"
  glueJobName: string; // Dynamic name from TransformJobConstruct
}

export class OrchestratorConstruct extends Construct {
  constructor(scope: Construct, id: string, props: OrchestratorProps) {
    super(scope, id);

    const { environment, dataBucket, cleanPrefix, glueJobName } = props;

    // ✅ EventBridge rule with dynamic name
    const rule = new events.Rule(this, "OnCsvArrived", {
      ruleName: `${environment}-cat-on-csv-arrived`, // ✅ Dynamic name
      description: `Trigger Glue ETL-2 when CSV arrives in ${cleanPrefix} (${environment})`,
      eventPattern: {
        source: ["aws.s3"],
        detailType: ["Object Created"],
        detail: {
          bucket: { name: [dataBucket.bucketName] },
          object: { key: [{ prefix: cleanPrefix }] },
        },
      },
    });

    // Create an IAM role for EventBridge to invoke Glue
    const eventBridgeGlueRole = new iam.Role(this, "EventBridgeGlueRole", {
      roleName: `${environment}-eventbridge-glue-role`,
      assumedBy: new iam.ServicePrincipal("events.amazonaws.com"),
      description: `Role for EventBridge to start Glue jobs (${environment})`
    });

    eventBridgeGlueRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["glue:StartJobRun"],
        resources: ["*"],
      })
    );

    // ✅ Target uses dynamic glueJobName
    rule.addTarget(new targets.AwsApi({
      service: "Glue",
      action: "startJobRun",
      parameters: { JobName: glueJobName }, // ✅ Uses dynamic name from construct
      policyStatement: new iam.PolicyStatement({
        actions: ["glue:StartJobRun"],
        resources: ["*"],
      }),
    }));
  }
}