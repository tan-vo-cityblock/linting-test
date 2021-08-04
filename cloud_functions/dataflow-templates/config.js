module.exports = {
  production: {
    requestParameters: {
      environment: "prod",
      tempLocation: "gs://cityblock-data-dataflow-temp/temp",
      patientDataBucket: "gs://cityblock-production-patient-data",
      medicalDataset: "medical",
      clinicalInfoUpdateTopic: "clinicalInformationUpdateMessages",
      redoxMessagesTable: "redox_messages",
      streaming: "false",
    },
    projectId: "cityblock-data",
    location: "us-central1",
  },
  staging: {
    requestParameters: {
      environment: "staging",
      tempLocation: "gs://cityblock-staging-data-dataflow-temp/temp",
      patientDataBucket: "gs://cityblock-staging-patient-data",
      medicalDataset: "medical",
      clinicalInfoUpdateTopic: "stagingClinicalInformationUpdateMessages",
      redoxMessagesTable: "redox_messages",
      streaming: "false",
    },
    projectId: "staging-cityblock-data",
    location: "us-central1",
  },
  development: {
    requestParameters: {
      environment: "staging",
      tempLocation: "gs://internal-tmp-cbh-staging/temp",
      patientDataBucket: "gs://dev-cityblock-production-patient-data",
      medicalDataset: "dev_medical",
      clinicalInfoUpdateTopic: "devClinicalInformationUpdateMessages",
      redoxMessagesTable: "dev_redox_messages",
      streaming: "false",
    },
    projectId: "staging-cityblock-data",
    location: "us-central1",
  },
  templateFileLocations: {
    ccdRefresh:
      "gs://cityblock-data-dataflow-temp/templates/clinical-summary-refresher.json",
  },
};
