const config = require("../config.js")[process.env.ENV];
const axios = require("axios");

const CREATE_OR_UPDATE_ENDPOINT = "/api/2014-08-01/patients";

const SEX_MAPPINGS = {
  male: "M",
  female: "F",
};

class CureatrAPI {
  constructor(auth) {
    if (!(auth.username && auth.password)) {
      throw new Error("Missing authentication information for Cureatr API.");
    }

    this.client = axios.create({ baseURL: config.cureatrHost });
    this.auth = auth;
  }

  async updateOrCreate(attributionInfo) {
    this._validate(attributionInfo);

    const requestBody = this._transform(attributionInfo);
    const response = await this.client.post(
      CREATE_OR_UPDATE_ENDPOINT,
      requestBody,
      {
        auth: this.auth,
      }
    );

    if (response.data.status === "error") {
      throw new Error(
        `Error updating or creating member in Cureatr: [requestBody: ${JSON.stringify(
          requestBody
        )}] [response: ${response.data.message}]`
      );
    }

    return response;
  }

  // Private methods
  _transform(attributionInfo) {
    const {
      patientId,
      dob,
      gender,
      firstName,
      middleName,
      lastName,
      city,
      state,
      zip,
      addressLine1,
      addressLine2,
      marketId,
    } = attributionInfo;
    const associated_ou = [config.marketMappings[marketId]];
    const pcm_enabled = true;
    const suffixRe = /(?<suffix>(?: jr\.?| sr\.?| iv| iii| ii| v|){1,})$/i;
    const rawSuffix = suffixRe.exec(lastName).groups.suffix;
    const name = {
      given: firstName,
      family: lastName.replace(suffixRe, ""),
      ...(!!middleName && { middle: middleName }),
      ...(!!rawSuffix && { suffix: rawSuffix.split(" ").filter(Boolean)[0] }),
    };
    const sex = SEX_MAPPINGS[lower(gender)] || "U";
    const formattedAddressLine2 = addressLine2 ? `, ${addressLine2}` : "";
    const street_address = `${addressLine1}${formattedAddressLine2}`;
    const address = {
      street_address,
      city,
      state,
      zip,
      country: "us",
    };

    return {
      pid: patientId,
      dob,
      sex,
      name,
      address,
      pcm_enabled,
      associated_ou,
    };
  }

  _validate(attributionInfo) {
    const {
      patientId,
      dob,
      gender,
      firstName,
      lastName,
      city,
      state,
      zip,
      addressLine1,
      marketId,
    } = attributionInfo;
    if (
      !(
        patientId &&
        dob &&
        gender &&
        firstName &&
        lastName &&
        city &&
        state &&
        zip &&
        addressLine1 &&
        marketId
      )
    ) {
      throw new TypeError(`One of the following fields is MISSING:
        [patientId: ${patientId}], [dob: ${dob}], [gender: ${gender}], [firstName: ${firstName}], [lastName: ${lastName}], [city: ${city}], [state: ${state}], [zip: ${zip}], [addressLine1: ${addressLine1}], [marketId: ${marketId}]`);
    }
  }
}

module.exports = CureatrAPI;
