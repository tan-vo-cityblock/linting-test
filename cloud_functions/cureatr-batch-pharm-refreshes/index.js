const { Storage } = require("@google-cloud/storage");
const { v1 } = require("@google-cloud/pubsub");
const { Readable } = require("stream");
const { format } = require("date-fns");
const {
  iid,
  npi,
  npiName,
  bucketName,
  projectId,
  maxMessages,
  subscriptionName,
} = require("./config.js");

exports.writeMedsRefreshFile = async (req, res) => {
  const subClient = new v1.SubscriberClient();

  const formattedSubscription = subClient.subscriptionPath(
    projectId,
    subscriptionName
  );

  const request = {
    subscription: formattedSubscription,
    maxMessages,
  };

  const [response] = await subClient.pull(request);

  const timestamp = format(new Date(), "yyyyMMddHHmmssSSS");
  const filePath = `patients_rx_cityblock_${timestamp}.csv`;

  const { prescriptionSubscription, ackIds } = await readPatientsIntoMem(
    response
  );
  console.log("Done reading messages into memory");

  if (!ackIds.length) {
    console.log("Nothing to write to Cureatr SFTP at this time");
    return res
      .status(200)
      .send("Nothing to write to Cureatr SFTP at this time");
  }

  const storage = new Storage();

  const cureatrWrite = await storage
    .bucket(bucketName)
    .file(filePath)
    .createWriteStream();
  prescriptionSubscription
    .pipe(cureatrWrite)
    .on("error", (err) => {
      console.error(
        `Error writing Cureatr pharmacy refresh file for file: [filePath: ${filePath}]`
      );
      console.error(JSON.stringify(err));
      return res
        .status(500)
        .send(
          `Error writing Cureatr pharmacy refresh file for file: [filepath: ${filePath}]`
        );
    })
    .on("finish", async () => {
      console.log(
        `Successfully wrote Cureatr pharmacy refresh file for patient: [filepath: ${filePath}]`
      );
      const ackRequest = {
        subscription: formattedSubscription,
        ackIds: ackIds,
      };
      await subClient.acknowledge(ackRequest);
      return res
        .status(200)
        .send(
          `Successfully wrote Cureatr pharmacy refresh file for patient: [filepath: ${filePath}]`
        );
    });
};

const readPatientsIntoMem = async (pubsubResponse) => {
  const header = "iid,pid,npi,npi_name";
  const rows = new Set();
  const ackIds = [];
  for (const message of pubsubResponse.receivedMessages) {
    try {
      const data = Buffer.from(message.message.data, "base64").toString(
        "utf-8"
      );
      console.log(`Received message: ${JSON.parse(data)}`);
      const { patientId } = JSON.parse(data);
      rows.add(`${iid},${patientId},${npi},${npiName}`);
      ackIds.push(message.ackId);
    } catch (err) {
      console.error(
        `Error writing Cureatr pharmacy refresh file for message: [message: ${JSON.stringify(
          message.message
        )}]`
      );
    }
  }
  return {
    prescriptionSubscription: Readable.from(
      `${header}\n${[...rows].join("\n")}`
    ),
    ackIds,
  };
};
