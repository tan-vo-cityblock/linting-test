/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} pubSubEvent The event payload.
 * @param {object} context The event metadata.
 */

const config = require("./config.js")[process.env.ENV];
const HealthGorillaAPI = require("./healthgorilla-api/index.js");
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");

exports.updateOrCreateMember = async (req, res) => {
  const secretsClient = new SecretManagerServiceClient();
  const [healthgorillaVersion] = await secretsClient.accessSecretVersion({
    name: `projects/${config.secretsProjectName}/secrets/${config.secretName}/versions/1`,
  });
  const healthGorillaAuth = JSON.parse(
    healthgorillaVersion.payload.data.toString("utf-8")
  );

  const messageData = Buffer.from(req.body.message.data, "base64").toString(
    "utf-8"
  );
  const messagePayload = JSON.parse(messageData);

  try {
    console.log("Authenticating with HealthGorilla");
    const healthGorillaClient = new HealthGorillaAPI(healthGorillaAuth);
    await healthGorillaClient.authenticateClient();

    console.log(
      `Updating member in HealthGorilla for: [patientId: ${messagePayload.patientId}, mrn: ${messagePayload.mrn}]`
    );
    await healthGorillaClient.updateOrCreate(messagePayload);

    console.log(
      `Successfully synced member with HealthGorilla for: [patientId: ${messagePayload.patientId}, mrn: ${messagePayload.mrn}]`
    );
    return res
      .status(200)
      .send(
        `Updated member in HealthGorilla for [patientId: ${messagePayload.patientId}]`
      );
  } catch (err) {
    console.log(err.message);
    if (err instanceof TypeError) {
      return res.status(400).send(err.message);
    }
    return res.status(500).send(err.message);
  }
};
