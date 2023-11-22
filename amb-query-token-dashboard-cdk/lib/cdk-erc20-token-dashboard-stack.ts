import * as cdk from 'aws-cdk-lib';
import * as glueAlpha from '@aws-cdk/aws-glue-alpha';
import * as path from 'path';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

export class CdkErc20TokenDashboardStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const TOKEN_ADDRESS = '0x6c3ea9036406852006290770bedfcaba0e23a0e8'

    //S3 bucket
    const glueJobOutput = new s3.Bucket(this, 'Bucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const BucketGlueJobOutput = glueJobOutput.bucketName

    const glueJob = new glueAlpha.Job(this, 'EnableSparkUI', {
      jobName: 'token-snapshots-cdk',
      sparkUI: {
        enabled: true,
      },
      executable: glueAlpha.JobExecutable.pythonEtl({
        glueVersion: glueAlpha.GlueVersion.V3_0,
        pythonVersion: glueAlpha.PythonVersion.THREE,
        script: glueAlpha.Code.fromAsset(path.join(__dirname, '..', 'glue', 'glue', 'token-snapshots.py')),
      }),
      defaultArguments: {
        '--s3_bucket_name': BucketGlueJobOutput,
        '--token': TOKEN_ADDRESS
      },
    });

    // Capture name 
    const tokenSnapshot = glueJob.jobName;
    const tokenSnapshotRole = glueJob.role;

    //glue job schedule
    new glue.CfnTrigger(this, 'GlueTrigger', {
      type: 'SCHEDULED',
      startOnCreation: true,
      actions: [
        {
          jobName: tokenSnapshot,
        }
      ],
      schedule: 'cron(0 0 * * ? *)'
    }
    )

    const iamPolicyStatementTokenSnapshot = new iam.PolicyStatement({
      actions: ['managedblockchain-query:ListTokenBalances', 'managedblockchain-query:ListTransactions', 'managedblockchain-query:ListTransactionEvents', 's3:*'],
      resources: ['*'],
      effect: iam.Effect.ALLOW
    });

    const iamPolicyTokenSnapshot = new iam.Policy(this, 'glue-job-amb-query', {
      statements: [
        iamPolicyStatementTokenSnapshot
      ]
    });

    tokenSnapshotRole.attachInlinePolicy(iamPolicyTokenSnapshot);
    new cdk.CfnOutput(this, `glue-job-role-arn`, {
      value: tokenSnapshotRole.roleArn
    });

    const glueJob2 = new glueAlpha.Job(this, 'EnableSparkUI2', {
      jobName: 'token-transfers-cdk',
      sparkUI: {
        enabled: true,
      },
      executable: glueAlpha.JobExecutable.pythonEtl({
        glueVersion: glueAlpha.GlueVersion.V3_0,
        pythonVersion: glueAlpha.PythonVersion.THREE,
        script: glueAlpha.Code.fromAsset(path.join(__dirname, '..', 'glue', 'glue', 'token-transfers.py')),
      }),
      defaultArguments: {
        '--s3_bucket_name': BucketGlueJobOutput,
        '--token': TOKEN_ADDRESS
      },
    });

    // Capture name 
    const tokenTransfers = glueJob2.jobName;
    const tokenTransfersRole = glueJob2.role;

    //glue job schedule using CfnTrigger
    new glue.CfnTrigger(this, 'GlueTrigger2', {
      type: 'SCHEDULED',
      startOnCreation: true,
      actions: [
        {
          jobName: tokenTransfers,
        }
      ],
      schedule: 'cron(0 0/1 * * ? *)'
    }
    )

    const iamPolicyStatementTokenTransfers = new iam.PolicyStatement({
      actions: ['managedblockchain-query:ListTokenBalances', 'managedblockchain-query:ListTransactions', 'managedblockchain-query:ListTransactionEvents', 's3:*', 'ssm:*'],
      resources: ['*'],
      effect: iam.Effect.ALLOW
    });

    const iamPolicyTokenTransfers = new iam.Policy(this, 'glue-job-amb-query2', {
      statements: [
        iamPolicyStatementTokenTransfers
      ]
    });

    tokenTransfersRole.attachInlinePolicy(iamPolicyTokenTransfers);
    new cdk.CfnOutput(this, `glue-job-role-arn2`, {
      value: tokenTransfersRole.roleArn
    });
  }
}