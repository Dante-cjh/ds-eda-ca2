import {Context, SNSHandler, SQSEvent, SQSHandler} from "aws-lambda";
import {
    GetObjectCommand,
    GetObjectCommandInput,
    S3Client,
} from "@aws-sdk/client-s3";
import * as path from "path";

const s3 = new S3Client();

import {DynamoDBClient, PutItemCommand} from "@aws-sdk/client-dynamodb";
import {DeleteCommand, DynamoDBDocumentClient, PutCommand} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: SNSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const snsMessage = JSON.parse(record.Sns.Message);
        if (snsMessage) {
            console.log("SnsMessage", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                console.log("s3e", JSON.stringify(s3e));
                // Object key may have spaces or unicode non-ASCII characters.
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));
                const extension = path.extname(srcKey);
                try {
                    console.log("srcKey", srcKey);
                    await ddbDocClient.send(
                        new DeleteCommand({
                            TableName: process.env.TABLE_NAME,
                            Key: {FileName: srcKey},
                        })
                    );
                    console.log("Deleted image from table");
                } catch (error) {
                    console.log(error);
                }
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