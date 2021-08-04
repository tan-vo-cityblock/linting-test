exports.uploadToCureatr = async (file) => {
  const { Storage } = require("@google-cloud/storage");
  const {
    SecretManagerServiceClient,
  } = require("@google-cloud/secret-manager");
  const Client = require("ssh2-sftp-client");
  const path = require("path");
  const {
    host,
    port,
    username,
    secretsProjectName,
    secretName,
  } = require("./config.js");

  const storage = new Storage();
  const bucketName = file.bucket;
  const filePath = file.name;

  const secretsClient = new SecretManagerServiceClient();
  const [version] = await secretsClient.accessSecretVersion({
    name: `projects/${secretsProjectName}/secrets/${secretName}/versions/1`,
  });
  const privateKey = version.payload.data.toString("utf-8");

  const sftpConfig = { host, port, username, privateKey };

  const readStream = await storage
    .bucket(bucketName)
    .file(filePath)
    .createReadStream()
    .on("error", (err) => {
      console.error(JSON.stringify(err));
      throw new Error(
        `Error uploading meds refresh file to Cureatr: [filePath: ${filePath}]`
      );
    });

  const baseFileName = path.parse(filePath).name;

  const cureatrSftp = new Client();
  return cureatrSftp
    .connect(sftpConfig)
    .then(() => {
      return cureatrSftp.put(readStream, `${baseFileName}.tmp`);
    })
    .then(() => {
      console.log(
        `${baseFileName}.tmp has been written to Cureatr SFTP bucket`
      );
    })
    .then(() => {
      return cureatrSftp.rename(`${baseFileName}.tmp`, `${baseFileName}.csv`);
    })
    .then(() => {
      console.log(
        `${baseFileName}.csv has been written to Cureatr SFTP bucket`
      );
    })
    .then(() => {
      console.log(
        `SUCCESS uploading [filePath: ${filePath}] to Cureatr server`
      );
      return cureatrSftp.end();
    })
    .catch((err) => {
      console.error(JSON.stringify(err));
      throw new Error(
        `Error uploading meds refresh file to Cureatr: [filePath: ${filePath}]`
      );
    });
};
