module.exports = {
  production: {
    healthGorillaHost: "https://www.healthgorilla.com/",
    healthGorillaAudience: "logan.hasson",
    invalidMarketIds: [
      "3d8e4e1d-bdb5-4676-9e25-9b6fa248101f",
      "6a4df00d-6570-4f03-9d29-de01cc22b128",
    ],
    secretsProjectName: "cbh-secrets",
    secretName: "prod-healthgorilla-api-creds",
  },
  test: {
    healthGorillaHost: "https://sandbox.healthgorilla.com/",
    healthGorillaAudience: "laurenfr",
    invalidMarketIds: [
      "1bf5a200-cdb9-414d-9778-a584cd5bef8c",
      "ea2655eb-5411-4a76-bc63-89aa79a3f64c",
    ],
    secretsProjectName: "cbh-secrets",
    secretName: "dev-healthgorilla-api-creds",
  },
};
