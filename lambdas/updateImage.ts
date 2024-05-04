import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DeleteCommand, DynamoDBDocumentClient, UpdateCommand} from "@aws-sdk/lib-dynamodb";
import {GetObjectCommand, GetObjectCommandInput, S3Client} from "@aws-sdk/client-s3";
import {SNSHandler} from "aws-lambda";


const ddbDocClient = createDDbDocClient();
const s3 = new S3Client();

export const handler: SNSHandler = async (event) => {
    console.log("Event ", JSON.stringify(event));
    for (const record of event.Records) {
        const snsMessage = JSON.parse(record.Sns.Message);
        const snsMessageAttributes = record.Sns.MessageAttributes;
        if (snsMessageAttributes.comment_type.Value === "Caption") {
            console.log("SnsMessage", JSON.stringify(snsMessage));
            const srcKey = snsMessage.name;
            const description = snsMessage.description;
            try {
                console.log("srcKey", srcKey);
                await ddbDocClient.send(
                    new UpdateCommand({
                        TableName: process.env.TABLE_NAME,
                        Key: {FileName: srcKey},
                        UpdateExpression: "SET Description = :Description",
                        ExpressionAttributeValues: {
                            ':Description': description
                        },
                    })
                );
                console.log("Update image from table");
            } catch (error) {
                console.log(error);
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