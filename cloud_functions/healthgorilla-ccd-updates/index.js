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
const ElationAPI = require("./elation-api/index.js");
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");

exports.uploadCCDA = async (req, res) => {
  const secretsClient = new SecretManagerServiceClient();
  const [elationVersion] = await secretsClient.accessSecretVersion({
    name: `projects/${config.secretsProjectName}/secrets/${config.elationSecretName}/versions/1`,
  });
  const [healthGorillaVersion] = await secretsClient.accessSecretVersion({
    name: `projects/${config.secretsProjectName}/secrets/${config.healthGorillaSecretName}/versions/1`,
  });
  const elationAuth = JSON.parse(elationVersion.payload.data.toString("utf-8"));
  const healthGorillaAuth = JSON.parse(
    healthGorillaVersion.payload.data.toString("utf-8")
  );

  const requestData = Buffer.from(req.body.message.data, "base64").toString(
    "utf-8"
  );
  const {
    resource,
    action,
    data: { id, patient },
  } = JSON.parse(requestData);
  const mrn = resource === "patients" ? id : patient;
  const eventType = `${resource}_${action}`;

  if (config.validEventTypes.includes(eventType)) {
    try {
      console.log("Authenticating with Elation and HealthGorilla");
      const elationClient = new ElationAPI(elationAuth);
      await elationClient.authenticateClient();
      const healthGorillaClient = new HealthGorillaAPI(healthGorillaAuth);
      await healthGorillaClient.authenticateClient();

      console.log(`Pulling CCDA for member for: [mrn: ${mrn}]`);
      const base64ccd = await elationClient.getCCDA(mrn);
      console.log(
        `Uploading CCDA to HealthGorilla for member for: [mrn: ${mrn}]`
      );
      await healthGorillaClient.uploadCCDA(mrn, base64ccd);

      console.log(
        `Successfully uploaded CCDA for member in Health Gorilla for [mrn: ${mrn}]`
      );
      return res
        .status(200)
        .send(`Uploaded CCDA for member in Health Gorilla for [mrn: ${mrn}]`);
    } catch (err) {
      console.log(err.message);
      if (err instanceof TypeError) {
        return res.status(400).send(err.message);
      }
      return res.status(500).send(err.message);
    }
  } else {
    console.log(
      `Will not upload CCDA for [eventType: ${eventType}, mrn: ${mrn}]`
    );
    return res
      .status(200)
      .send(`Will not upload CCDA for [eventType: ${eventType}, mrn: ${mrn}]`);
  }
};
