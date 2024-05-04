import type {DynamoDBStreamHandler, SQSHandler} from "aws-lambda";
import {SES_EMAIL_FROM, SES_EMAIL_TO, SES_REGION} from "../env";
import {SendEmailCommand, SendEmailCommandInput, SESClient} from "@aws-sdk/client-ses";

if (!SES_EMAIL_TO || !SES_EMAIL_FROM || !SES_REGION) {
    throw new Error(
        "Please add the SES_EMAIL_TO, SES_EMAIL_FROM and SES_REGION environment variables in an env.js file located in the root directory"
    );
}

type ContactDetails = {
    name: string;
    email: string;
    message: string;
};

const client = new SESClient({region: SES_REGION});

export const handler: DynamoDBStreamHandler = async (event: any) => {
    console.log("Received event", JSON.stringify(event, null, 2));
    for (const record of event.Records) {
        if (record.eventName == "REMOVE") {
            try {
                const {name, email, message}: ContactDetails = {
                    name: "The Photo Album delete image",
                    email: SES_EMAIL_FROM,
                    message: `The file has deleted successfully.`,
                };
                const params = sendEmailParams({name, email, message});
                await client.send(new SendEmailCommand(params));
            } catch (error: unknown) {
                console.log("ERROR is: ", error);
                // return;
            }
        }
    }
}

function sendEmailParams({name, email, message}: ContactDetails) {
    const parameters: SendEmailCommandInput = {
        Destination: {
            ToAddresses: [SES_EMAIL_TO],
        },
        Message: {
            Body: {
                Html: {
                    Charset: "UTF-8",
                    Data: getHtmlContent({name, email, message}),
                },
            },
            Subject: {
                Charset: "UTF-8",
                Data: `Delete image successfully!!!❤️`,
            },
        },
        Source: SES_EMAIL_FROM,
    };
    return parameters;
}

function getHtmlContent({name, email, message}: ContactDetails) {
    return `
    <html>
      <body>
        <h2>Sent from: </h2>
        <ul>
          <li style="font-size:18px">👤 <b>${name}</b></li>
          <li style="font-size:18px">✉️ <b>${email}</b></li>
        </ul>
        <p style="font-size:18px">${message}</p>
      </body>
    </html> 
  `;
}