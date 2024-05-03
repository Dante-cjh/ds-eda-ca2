import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

import {Construct} from "constructs";

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Tables
        const FileTable = new dynamodb.Table(this, 'FileTable', {
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            partitionKey: { name: 'FileName', type: dynamodb.AttributeType.STRING },
            tableName: 'FileTable',
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        const imagesBucket = new s3.Bucket(this, "images", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            publicReadAccess: false,
        });

        // Integration infrastructure

        // rejected mails queue
        const rejectedMailsQueue = new sqs.Queue(this, "rejected-mailer-queue", {
            receiveMessageWaitTime: cdk.Duration.seconds(10),
        });

        const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
            receiveMessageWaitTime: cdk.Duration.seconds(10),
            deadLetterQueue: {
                queue: rejectedMailsQueue,
                // # of rejections by consumer (lambda function)
                maxReceiveCount: 1,
            },
        });

        const newImageTopic = new sns.Topic(this, "NewImageTopic", {
            displayName: "New Image topic",
        });

        const mailerQ = new sqs.Queue(this, "mailer-queue", {
            receiveMessageWaitTime: cdk.Duration.seconds(10),
        });

        // Lambda functions

        const processImageFn = new lambdanode.NodejsFunction(
            this,
            "ProcessImageFn",
            {
                runtime: lambda.Runtime.NODEJS_18_X,
                entry: `${__dirname}/../lambdas/processImage.ts`,
                timeout: cdk.Duration.seconds(15),
                memorySize: 128,
                environment: {
                    REGION: cdk.Aws.REGION,
                    TABLE_NAME: FileTable.tableName,
                },
            }
        );

        const mailerFn = new lambdanode.NodejsFunction(this, "mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/mailer.ts`,
        });

        const rejectionMailerFn = new lambdanode.NodejsFunction(this, "rejection-mailer-function", {
            runtime: lambda.Runtime.NODEJS_16_X,
            memorySize: 1024,
            timeout: cdk.Duration.seconds(3),
            entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
        });

        // S3 --> SQS
        imagesBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.SnsDestination(newImageTopic)  // Changed
        );

        // SQS --> Lambda
        const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });
        const newImageMailEventSource = new events.SqsEventSource(mailerQ, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });
        const newImageRejectionMailEventSource = new events.SqsEventSource(rejectedMailsQueue, {
            batchSize: 5,
            maxBatchingWindow: cdk.Duration.seconds(10),
        });

        newImageTopic.addSubscription(
            new subs.SqsSubscription(imageProcessQueue)
        );
        newImageTopic.addSubscription(new subs.SqsSubscription(mailerQ));

        processImageFn.addEventSource(newImageEventSource);
        mailerFn.addEventSource(newImageMailEventSource);
        rejectionMailerFn.addEventSource(newImageRejectionMailEventSource);

        // Permissions

        imagesBucket.grantRead(processImageFn);
        mailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );
        // Add SES permissions to the rejection mailer function
        rejectionMailerFn.addToRolePolicy(
            new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                actions: [
                    "ses:SendEmail",
                    "ses:SendRawEmail",
                    "ses:SendTemplatedEmail",
                ],
                resources: ["*"],
            })
        );

        // Grant the processImageFn function write access to the DynamoDB table
        FileTable.grantReadWriteData(processImageFn);

        // Output

        new cdk.CfnOutput(this, "bucketName", {
            value: imagesBucket.bucketName,
        });
    }
}
