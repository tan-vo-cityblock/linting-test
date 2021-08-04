const { PubSub } = require("@google-cloud/pubsub");

const pubsub = new PubSub();
const topicName = "elationEvents";

exports.elationHookEventReceiver = async (req, res) => {
  if (req.method !== "POST") {
    // Return a "method not allowed" error
    return res.status(405).end();
  }

  let bodyData;
  let messageId;
  let resource;
  let action;

  switch (req.get("content-type")) {
    case "application/json":
      bodyData = req.body;
      messageId = req.body.event_id.toString();
      resource = req.body.resource;
      action = req.body.action;
      break;
    default:
      console.error(
        new Error("elation_hook is receiving non-json media type POST.")
      );
      // Return an "unsuported media type" error
      return res.status(415).end();
  }

  const dataBuffer = Buffer.from(JSON.stringify(bodyData));
  const attributes = {
    messageId: messageId,
    resource: resource,
    action: action,
  };

  console.info("Received message from Elation: ", attributes);

  //Force elation to retry message if we have issues relaying message to pubsub.
  try {
    const pubSubMessageId = await pubsub
      .topic(topicName)
      .publish(dataBuffer, attributes);
    res.sendStatus(200);
  } catch (e) {
    console.error(new Error(e.message));
    //Return an "internal server" error
    res.sendStatus(500);
  }
};
