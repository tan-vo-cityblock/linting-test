/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} pubSubEvent The event payload.
 * @param {object} context The event metadata.
 */

exports.updateOrCreateMember = async (req, res) => {
  const config = require("./config.js")[process.env.ENV];
  const CureatrAPI = require("./cureatr-api/index.js");
  const {
    SecretManagerServiceClient,
  } = require("@google-cloud/secret-manager");

  const secretsClient = new SecretManagerServiceClient();
  const [version] = await secretsClient.accessSecretVersion({
    name: `projects/${config.secretsProjectName}/secrets/${config.secretName}/versions/2`,
  });
  const cureatorAuth = JSON.parse(version.payload.data.toString("utf-8"));

  const data = Buffer.from(req.body.message.data, "base64").toString("utf-8");
  const payload = JSON.parse(data);

  try {
    const cureatrClient = new CureatrAPI(cureatorAuth);
    await cureatrClient.updateOrCreate(payload);
    return res
      .status(200)
      .send(`Updated member in Cureatr for [patientId: ${payload.patientId}]`);
  } catch (err) {
    if (err instanceof TypeError) {
      return res.status(400).send(err.message);
    }
    return res.status(500).send(err.message);
  }
};
