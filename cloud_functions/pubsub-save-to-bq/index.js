/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} pubSubEvent The event payload.
 * @param {object} context The event metadata.
 */

exports.saveMessageToBigQuery = async (pubSubEvent, context) => {
  const BigQuery = require("@google-cloud/bigquery");

  const bigqueryClient = new BigQuery({
    projectId: process.env.BIGQUERY_PROJECT_ID,
  });
  const pubsubDataset = bigqueryClient.dataset(process.env.BIGQUERY_DATASET);
  const pubsubTable = pubsubDataset.table(process.env.BIGQUERY_TABLE);

  console.log(
    "pubsub context passed to cloud function",
    JSON.stringify(context)
  );

  const insertedAt = bigqueryClient.timestamp(context.timestamp);
  const publishedAt = bigqueryClient.timestamp(
    pubSubEvent.attributes.CloudPubSubDeadLetterSourceTopicPublishTime
  );
  const eventId = context.eventId;
  const hmac = pubSubEvent.attributes.hmac;
  const source =
    pubSubEvent.attributes.CloudPubSubDeadLetterSourceSubscription ||
    (context.resource && context.resource.name);
  const payload = Buffer.from(pubSubEvent.data, "base64").toString();
  const bqRow = {
    event_id: eventId,
    source,
    hmac,
    payload,
    published_at_source: publishedAt,
    inserted_at: insertedAt,
  };

  try {
    await pubsubTable.insert(bqRow);
  } catch (err) {
    console.error(
      "Received error saving PubSub message to BigQuery:",
      JSON.stringify(err.errors)
    );
  }

  return;
};
