import StorageEvent = GoogleCloudPlatform.CloudFunctions.StorageEvent;
import Callback = GoogleCloudPlatform.CloudFunctions.Callback;
import { IncomingWebhook } from "@slack/client";

import settings from "./settings.json";

const url = settings.SLACK_WEBHOOK_URL;
const webhook = new IncomingWebhook(url);

export function gcsPubsubSlack(event: StorageEvent, callback: Callback) {
  webhook.send("```" + JSON.stringify(event) + "```", function (err, res) {
    if (err) {
      console.log("Error:", err);
    } else {
      console.log("Message sent: ", res);
    }

    callback();
  });
}
