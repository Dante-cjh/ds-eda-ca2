import {Context, SQSEvent, SQSHandler} from "aws-lambda";
import {
    GetObjectCommand,
    PutObjectCommandInput,
    GetObjectCommandInput,
    S3Client,
    PutObjectCommand,
} from "@aws-sdk/client-s3";
import * as path from "path";

const s3 = new S3Client();

import {APIGatewayProxyHandlerV2} from "aws-lambda";
import {DynamoDBClient, PutItemCommand} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, PutCommand} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SQSHandler = async (event: SQSEvent, context: Context) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const recordBody = JSON.parse(record.body);  // Parse SQS message
        const snsMessage = JSON.parse(recordBody.Message); // Parse SNS message

        if (snsMessage.Records) {
            console.log("Record body ", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcBucket = s3e.bucket.name;
                // Object key may have spaces or unicode non-ASCII characters.
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));
                let origimage = null;
                // Download the image from the S3 source bucket.
                const params: GetObjectCommandInput = {
                    Bucket: srcBucket,
                    Key: srcKey,
                };

                // jpg and png only
                const extension = path.extname(srcKey);
                console.log("extension", extension);
                if (extension !== '.jpeg' && extension !== '.png') {
                    throw new Error(`Invalid file type: ${extension}`);
                }

                await ddbDocClient.send(
                    new PutItemCommand({
                        TableName: process.env.TABLE_NAME,
                        Item: {FileName: {S: srcKey}},
                    })
                );

                origimage = await s3.send(new GetObjectCommand(params));
                // Process the image ......
            }
        }
    }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}