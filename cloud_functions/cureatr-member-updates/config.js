module.exports = {
  production: {
    cureatrHost: "https://api.cureatr.com/",
    marketMappings: {
      "9065e1fc-2659-4828-9519-3ad49abf5126": "cityblockct",
      "76150da6-b62a-4099-9bd6-42e7be3ffc62": "cityblockma",
      "31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04": "cityblocknc",
      "3d8e4e1d-bdb5-4676-9e25-9b6fa248101f": "cityblockny",
      "6a4df00d-6570-4f03-9d29-de01cc22b128": "cityblockma",
      "31e02957-0bac-475b-b306-a6497a3cb823": "cityblockdc",
      "f765efc5-04a4-494a-aecc-dc150bd2907d": "cityblockny",
    },
    secretsProjectName: "cbh-secrets",
    secretName: "prod-cureatr-api-creds",
  },
  test: {
    cureatrHost: "https://api.play.cureatr.com/",
    marketMappings: {
      "6b7a43ac-7a2a-4ee5-97cf-480ddf8abf09": "cityblockma",
      "ea2655eb-5411-4a76-bc63-89aa79a3f64c": "cityblockma",
      "cb3d3c98-964a-47a2-a235-11d20b910d1c": "cityblockny",
      "ae050526-a951-4d07-b0ff-bd1d7239ac76": "cityblockdc",
      "05f99ce8-9680-44a2-855c-d8eb63dad7f5": "cityblocknc",
      "1bf5a200-cdb9-414d-9778-a584cd5bef8c": "cityblockny",
      "84e127f6-7c3a-4893-9921-3c52c3b8a1d8": "cityblockct",
    },
    secretsProjectName: "cbh-secrets",
    secretName: "dev-cureatr-api-creds",
  },
};
