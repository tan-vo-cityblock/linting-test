const config = require("../config.js")[process.env.ENV];
const axios = require("axios");

const OAUTH_ENDPOINT = "oauth2/token/";
const CCDA_ENDPOINT = "ccda";

class ElationAPI {
  constructor(auth) {
    if (
      !(auth.client_id && auth.client_secret && auth.username && auth.password)
    ) {
      throw new Error("Missing authentication information for Elation API.");
    }

    this.client = axios.create({ baseURL: config.elationHost });
    this.auth = auth;
  }

  async authenticateClient() {
    const { client_id, client_secret, username, password } = this.auth;
    const params = new URLSearchParams({
      grant_type: "password",
      username,
      password,
    });
    const requestConfig = {
      auth: {
        username: client_id,
        password: client_secret,
      },
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
    };
    const { data } = await this.client.post(
      OAUTH_ENDPOINT,
      params,
      requestConfig
    );
    if (data.status === "error") {
      throw new Error(
        `Error authenticating into Elation: [response: ${data.message}]`
      );
    }
    this.client.defaults.headers.Authorization = `Bearer ${data.access_token}`;
    return data;
  }

  async getCCDA(mrn) {
    const { data } = await this.client.get(`${CCDA_ENDPOINT}/${mrn}`, {
      params: {
        sections: config.validCCDSectionTypes.join(","),
      },
    });

    if (data.status === "error") {
      throw new Error(
        `Error getting CCDA from Elation: [response: ${data.message}]`
      );
    }

    console.log(`Successfully pulled CCDA for member for: [mrn: ${mrn}]`);
    return data.base64_ccda;
  }
}

module.exports = ElationAPI;
