import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as codepipeline from 'aws-cdk-lib/aws-codepipeline';
import * as codepipeline_actions from 'aws-cdk-lib/aws-codepipeline-actions';
import * as codebuild from 'aws-cdk-lib/aws-codebuild';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import { PipelineConfig } from './configs/pipeline-config.interface';

export class PipelineStack extends cdk.Stack {
  public readonly pipeline: codepipeline.Pipeline;
  public readonly artifactBucket: s3.Bucket;

  constructor(
    scope: Construct,
    id: string,
    config: PipelineConfig,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    // Validate configuration
    this.validateConfig(config);

    // 📦 Create S3 bucket for pipeline artifacts
    this.artifactBucket = this.createArtifactBucket(config);

    // 🔐 Create IAM role for CDK deployments
    const cdkDeployRole = this.createCDKDeployRole(config);

    // 🔔 Create SNS topic for notifications (if enabled)
    const notificationTopic = config.notifications?.enabled 
      ? this.createNotificationTopic(config) 
      : undefined;

    // 🏗️ Create CodeBuild projects
    const normalizeDeployProject = this.createNormalizeDeployProject(config, cdkDeployRole);
    const etlDeployProject = this.createETLDeployProject(config, cdkDeployRole);

    // 🔄 Create the CodePipeline
    this.pipeline = this.createPipeline(config, cdkDeployRole);

    // 📋 Add stages to pipeline
    this.addSourceStage(config);
    this.addBuildAndTestStage(config);
    
    if (config.requireManualApproval) {
      this.addManualApprovalStage(config, notificationTopic);
    }
    
    this.addDeployStage(config, normalizeDeployProject, etlDeployProject);

    // 🔔 Add pipeline notifications
    if (notificationTopic) {
      this.addPipelineNotifications(notificationTopic);
    }

    // 🏷️ Apply tags
    this.applyTags(config);

    // 📤 Create outputs
    this.createOutputs(config);
  }

  /**
   * Validate configuration
   */
  private validateConfig(config: PipelineConfig): void {
    const required = [
      'environment',
      'pipelineName',
      'artifactBucketName',
      'github'
    ];

    for (const field of required) {
      if (!config[field as keyof PipelineConfig]) {
        throw new Error(`Missing required configuration: ${field}`);
      }
    }

    // Validate GitHub connection ARN
    if (!config.github.connectionArn ) {
      throw new Error(`Invalid GitHub connectionArn. Please create a connection in AWS Console first.`);
    }
  }

  /**
   * Create S3 bucket for artifacts
   */
  private createArtifactBucket(config: PipelineConfig): s3.Bucket {
    const bucket = new s3.Bucket(this, 'CatNormalizeArtifactBucket', {
      bucketName: config.artifactBucketName,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          id: 'DeleteOldArtifacts',
          enabled: true,
          expiration: cdk.Duration.days(30),
          noncurrentVersionExpiration: cdk.Duration.days(7)
        }
      ]
    });

    cdk.Tags.of(bucket).add('ResourceType', 'STORAGE');
    cdk.Tags.of(bucket).add('Purpose', 'pipeline-artifacts');

    return bucket;
  }

  /**
   * Create IAM role for CDK deployments
   */
  private createCDKDeployRole(config: PipelineConfig): iam.Role {
    const role = new iam.Role(this, 'CatNormalizeCDKDeployRole', {
      roleName: `cat-${config.environment}-normalize-cdk-deploy-role`,
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal('codebuild.amazonaws.com'),
        new iam.ServicePrincipal('codepipeline.amazonaws.com')
      ),
      description: `Role for CDK deployments in ${config.environment} environment`,
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('PowerUserAccess'),
      ],
      inlinePolicies: {
        CDKBootstrapAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: ['sts:AssumeRole'],
              resources: [
                `arn:aws:iam::${this.account}:role/cdk-*`
              ],
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                'iam:PassRole',
                'iam:GetRole',
                'iam:CreateRole',
                'iam:AttachRolePolicy',
                'iam:PutRolePolicy'
              ],
              resources: ['*'],
            }),
          ],
        }),
      },
    });

    cdk.Tags.of(role).add('ResourceType', 'IAM-ROLE');
    cdk.Tags.of(role).add('Purpose', 'cdk-deployment');

    return role;
  }

  /**
   * Create SNS topic for pipeline notifications
   */
  private createNotificationTopic(config: PipelineConfig): sns.Topic {
    const topic = new sns.Topic(this, 'PipelineNotificationTopic', {
      topicName: `cat-${config.environment}-pipeline-notifications`,
      displayName: `CAT ${config.environment.toUpperCase()} Pipeline Notifications`
    });

    // Add email subscriptions if provided
    if (config.notifications?.emails) {
      config.notifications.emails.forEach((email, index) => {
        topic.addSubscription(
          new subscriptions.EmailSubscription(email)
        );
      });
    }

    cdk.Tags.of(topic).add('ResourceType', 'NOTIFICATIONS');
    cdk.Tags.of(topic).add('Purpose', 'pipeline-alerts');

    return topic;
  }

  /**
   * Create CodeBuild project for Normalize Stack deployment
   */
  private createNormalizeDeployProject(
    config: PipelineConfig,
    role: iam.Role
  ): codebuild.PipelineProject {
    return new codebuild.PipelineProject(this, 'NormalizeDeployProject', {
      projectName: `cat-${config.environment}-normalize-deploy`,
      role: role,
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.SMALL,
        privileged: false,
      },
      environmentVariables: {
        ENVIRONMENT: {
          value: config.environment
        },
        AWS_REGION: {
          value: config.awsRegion
        }
      },
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            'runtime-versions': {
              nodejs: '20'
            },
            commands: [
              'echo "Installing AWS CDK..."',
              'npm install -g aws-cdk@2.102.0',
              'echo "CDK version: $(cdk --version)"'
            ]
          },
          pre_build: {
            commands: [
              'echo "Installing dependencies..."',
              'cd infrastructure',
              'npm ci',
              'echo "Listing files..."',
              'ls -la'
            ]
          },
          build: {
            commands: [
              'echo "Deploying Normalize Stack to $ENVIRONMENT..."',
              `cdk deploy cat-$ENVIRONMENT-normalize-stack -c environment=$ENVIRONMENT --require-approval never --verbose`,
              'echo "Normalize Stack deployment completed!"'
            ]
          }
        }
      }),
      cache: codebuild.Cache.local(codebuild.LocalCacheMode.SOURCE),
      timeout: cdk.Duration.minutes(30)
    });
  }

  /**
   * Create CodeBuild project for ETL Stack deployment
   */
  private createETLDeployProject(
    config: PipelineConfig,
    role: iam.Role
  ): codebuild.PipelineProject {
    return new codebuild.PipelineProject(this, 'ETLDeployProject', {
      projectName: `cat-${config.environment}-etl-deploy`,
      role: role,
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.SMALL,
        privileged: false,
      },
      environmentVariables: {
        ENVIRONMENT: {
          value: config.environment
        },
        AWS_REGION: {
          value: config.awsRegion
        }
      },
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            'runtime-versions': {
              nodejs: '20'
            },
            commands: [
              'echo "Installing AWS CDK..."',
              'npm install -g aws-cdk@2.102.0',
              'echo "CDK version: $(cdk --version)"'
            ]
          },
          pre_build: {
            commands: [
              'echo "Installing dependencies..."',
              'cd infrastructure',
              'npm ci'
            ]
          },
          build: {
            commands: [
              'echo "Deploying ETL Stack to $ENVIRONMENT..."',
              `cdk deploy cat-$ENVIRONMENT-etl2-stack -c environment=$ENVIRONMENT --require-approval never --verbose`,
              'echo "ETL Stack deployment completed!"'
            ]
          }
        }
      }),
      cache: codebuild.Cache.local(codebuild.LocalCacheMode.SOURCE),
      timeout: cdk.Duration.minutes(30)
    });
  }

  /**
   * Create the main CodePipeline
   */
  private createPipeline(
    config: PipelineConfig,
    role: iam.Role
  ): codepipeline.Pipeline {
    return new codepipeline.Pipeline(this, 'Pipeline', {
      pipelineName: config.pipelineName,
      artifactBucket: this.artifactBucket,
      role: role,
      pipelineType: codepipeline.PipelineType.V2,
      executionMode: codepipeline.ExecutionMode.QUEUED,
      restartExecutionOnUpdate: false
    });
  }

  /**
   * Add Source stage
   */
  private addSourceStage(config: PipelineConfig): void {
    const sourceOutput = new codepipeline.Artifact('SourceOutput');

    this.pipeline.addStage({
      stageName: 'Source',
      actions: [
        new codepipeline_actions.CodeStarConnectionsSourceAction({
          actionName: 'GitHub_Source',
          owner: config.github.owner,
          repo: config.github.repositoryName,
          branch: config.github.branchName,
          output: sourceOutput,
          connectionArn: config.github.connectionArn,
          triggerOnPush: true,
          codeBuildCloneOutput: true
        })
      ]
    });
  }

  /**
   * Add Build and Test stage
   */
  private addBuildAndTestStage(config: PipelineConfig): void {
    const sourceOutput = this.pipeline.stages[0].actions[0].actionProperties.outputs![0];

    const syntTestProject = new codebuild.PipelineProject(this, 'SynthTestProject', {
      projectName: `cat-${config.environment}-synth-test`,
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.SMALL,
      },
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            'runtime-versions': {
              nodejs: '20'
            },
            commands: [
              'npm install -g aws-cdk@2.102.0'
            ]
          },
          pre_build: {
            commands: [
              'cd infrastructure',
              'npm ci'
            ]
          },
          build: {
            commands: [
              'echo "Running CDK synth test..."',
              `cdk synth -c environment=${config.environment}`,
              'echo "CDK synth successful!"'
            ]
          }
        }
      })
    });

    this.pipeline.addStage({
      stageName: 'BuildAndTest',
      actions: [
        new codepipeline_actions.CodeBuildAction({
          actionName: 'CDK_Synth_Test',
          project: syntTestProject,
          input: sourceOutput,
        })
      ]
    });
  }

  /**
   * Add Manual Approval stage (if required)
   */
  private addManualApprovalStage(
    config: PipelineConfig,
    notificationTopic?: sns.Topic
  ): void {
    this.pipeline.addStage({
      stageName: 'ManualApproval',
      actions: [
        new codepipeline_actions.ManualApprovalAction({
          actionName: 'Approve_Deployment',
          notificationTopic: notificationTopic,
          additionalInformation: `Please review and approve deployment to ${config.environment.toUpperCase()} environment.\n\nThis will deploy:\n- Normalize Stack (ETL-1)\n- ETL Stack (ETL-2)`,
          externalEntityLink: config.github.repositoryUrl,
        })
      ]
    });
  }

  /**
   * Add Deploy stage
   */
  private addDeployStage(
    config: PipelineConfig,
    normalizeProject: codebuild.PipelineProject,
    etlProject: codebuild.PipelineProject
  ): void {
    const sourceOutput = this.pipeline.stages[0].actions[0].actionProperties.outputs![0];

    this.pipeline.addStage({
      stageName: 'Deploy',
      actions: [
        new codepipeline_actions.CodeBuildAction({
          actionName: 'Deploy_Normalize_Stack',
          project: normalizeProject,
          input: sourceOutput,
          runOrder: 1
        }),
        new codepipeline_actions.CodeBuildAction({
          actionName: 'Deploy_ETL_Stack',
          project: etlProject,
          input: sourceOutput,
          runOrder: 2 // Deploy after Normalize stack
        })
      ]
    });
  }

  /**
   * Add pipeline state change notifications
   */
  private addPipelineNotifications(topic: sns.Topic): void {
    this.pipeline.notifyOnExecutionStateChange(
      'PipelineStateChange',
      topic,
      {
        enabled: true,
        notificationRuleName: `cat-${this.pipeline.pipelineName}-state-change`
      }
    );

    this.pipeline.notifyOnAnyStageStateChange(
      'StageStateChange',
      topic,
      {
        enabled: true,
        notificationRuleName: `cat-${this.pipeline.pipelineName}-stage-change`
      }
    );
  }

  /**
   * Apply tags to all resources
   */
  private applyTags(config: PipelineConfig): void {
    if (config.tags) {
      Object.entries(config.tags).forEach(([key, value]) => {
        cdk.Tags.of(this).add(key, value);
      });
    }

    // Additional resource-specific tags
    cdk.Tags.of(this.pipeline).add('ResourceType', 'CI-CD-PIPELINE');
    cdk.Tags.of(this.pipeline).add('PipelineType', 'infrastructure-deployment');
  }

  /**
   * Create stack outputs
   */
  private createOutputs(config: PipelineConfig): void {
    new cdk.CfnOutput(this, 'Environment', {
      value: config.environment,
      description: 'Pipeline environment'
    });

    new cdk.CfnOutput(this, 'PipelineName', {
      value: this.pipeline.pipelineName,
      description: 'CodePipeline name'
    });

    new cdk.CfnOutput(this, 'PipelineArn', {
      value: this.pipeline.pipelineArn,
      description: 'CodePipeline ARN'
    });

    new cdk.CfnOutput(this, 'ArtifactBucket', {
      value: this.artifactBucket.bucketName,
      description: 'S3 bucket for pipeline artifacts'
    });

    new cdk.CfnOutput(this, 'GitHubRepository', {
      value: `${config.github.owner}/${config.github.repositoryName}`,
      description: 'GitHub repository'
    });

    new cdk.CfnOutput(this, 'GitHubBranch', {
      value: config.github.branchName,
      description: 'GitHub branch being monitored'
    });

    new cdk.CfnOutput(this, 'PipelineConsoleUrl', {
      value: `https://${config.awsRegion}.console.aws.amazon.com/codesuite/codepipeline/pipelines/${this.pipeline.pipelineName}/view`,
      description: 'AWS Console URL for the pipeline'
    });

    if (config.requireManualApproval) {
      new cdk.CfnOutput(this, 'ManualApprovalRequired', {
        value: 'Yes',
        description: 'Manual approval is required before deployment'
      });
    }
  }
}
