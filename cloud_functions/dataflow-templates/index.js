/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} pubSubEvent The event payload.
 * @param {object} context The event metadata.
 */

exports.runDataflowTemplate = async (req, res) => {
  const { google } = require("googleapis");
  const config = require("./config.js");

  const dataflow = google.dataflow("v1b3");
  const auth = new google.auth.GoogleAuth({
    scopes: [
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/userinfo.email",
    ],
  });
  const authClient = await auth.getClient();
  google.options({ auth: authClient });

  const { requestParameters, projectId, location } = config[process.env.ENV];
  const data = Buffer.from(req.body.message.data, "base64").toString("utf-8");
  const payload = JSON.parse(data);
  const { patientId, mrn: externalId, ehr: sourceName, jobName } = payload;

  if (!(patientId && externalId && sourceName && jobName)) {
    return res
      .status(400)
      .send(
        `Missing [patientId: ${patientId}], [mrn: ${externalId}], [ehr: ${sourceName}], or [jobName: ${jobName}]`
      );
  }
  try {
    await dataflow.projects.locations.flexTemplates.launch({
      projectId,
      location,
      requestBody: {
        launchParameter: {
          jobName: `${jobName.toLowerCase()}-${patientId}`,
          parameters: {
            ...requestParameters,
            patientId,
            externalId,
            sourceName,
          },
          containerSpecGcsPath: config.templateFileLocations[jobName],
        },
      },
    });
    return res
      .status(200)
      .send(`${jobName.toLowerCase()}-${patientId} job launched`);
  } catch (e) {
    return res.status(500).send(e);
  }
};
