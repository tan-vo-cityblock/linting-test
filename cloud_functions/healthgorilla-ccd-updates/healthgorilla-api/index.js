const config = require("../config.js")[process.env.ENV];
const axios = require("axios");
const jwt = require("jsonwebtoken");

const PATIENT_ENDPOINT = "/fhir/Patient";
const DOCUMENT_ENDPOINT = "/fhir/DocumentReference";
const OAUTH_ENDPOINT = "oauth/token";

const CITYBLOCK_DOMAIN = "https://cityblock.com/";
const ELATION_DOMAIN = "https://elationhealth.com/";

const SEX_MAPPINGS = {
  m: "male",
  f: "female",
  male: "male",
  female: "female",
};

class HealthGorillaAPI {
  constructor(auth) {
    if (!(auth.id && auth.secret)) {
      throw new Error(
        "Missing authentication information for Health Gorilla API."
      );
    }

    const now = Date.now();
    const fiveMinutes = 5 * 60 * 1000;
    this.jwt = jwt.sign(
      {
        iss: "https://www.cityblock.com/",
        aud: `${config.healthGorillaHost}${OAUTH_ENDPOINT}`,
        sub: config.healthGorillaAudience,
        exp: now + fiveMinutes,
        iat: now,
      },
      auth.secret
    );
    this.client = axios.create({ baseURL: config.healthGorillaHost });
    this.auth = auth;
  }

  async authenticateClient() {
    const params = new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      client_id: this.auth.id,
      assertion: this.jwt,
      scope: "place_orders get_orders patient360 user/*.*",
    });
    const requestConfig = {
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
        `Error authenticating into HealthGorilla: [response: ${data.message}]`
      );
    }
    this.client.defaults.headers.Authorization = `Bearer ${data.access_token}`;
    return data;
  }

  isInInvalidMarket(attributionInfo) {
    const isInvalidMarket = config.invalidMarketIds.includes(
      attributionInfo.marketId
    );
    if (isInvalidMarket) {
      console.log(
        `Member belongs to an invalid market and not a part of HealthGorilla workflow: [marketId: ${marketId}]`
      );
      return true;
    }
    return false;
  }

  async getByIdentifier(id, domain) {
    console.log(
      `Finding member by identifier for [id: ${id}, domain: ${domain}]`
    );
    const { data } = await this.client.get(PATIENT_ENDPOINT, {
      params: {
        identifier: `${domain}|${id}`,
      },
    });

    if (data.status === "error") {
      throw new Error(
        `Error getting member in HealthGorilla: [response: ${data.message}]`
      );
    }

    if (data.total == 0) {
      console.log(
        `Member has not had a profile made in health gorilla for: [id: ${id}, domain: ${domain}]`
      );
      return null;
    } else if (data.total == 1) {
      console.log(
        `Found member in HealthGorilla for: [id: ${id}, domain: ${domain}]`
      );
      return data.entry[0].resource;
    } else {
      throw new Error(
        `System expects only one Cityblock member per id: [total: ${data.total}] [id: ${id}] [domain: ${domain}]`
      );
    }
  }

  async updateOrCreate(attributionInfo) {
    console.log("Checking member home market");
    if (this.isInInvalidMarket(attributionInfo)) {
      return;
    }
    console.log("Validating attribution data");
    this._validate(attributionInfo);
    console.log("Transforming attribution data");
    const newMemberInfo = this._transformMemberData(attributionInfo);
    const { name, gender, birthDate, address } = newMemberInfo;

    const currMemberInfo = await this.getByIdentifier(
      attributionInfo.mrn,
      ELATION_DOMAIN
    );

    if (currMemberInfo) {
      const requestBody = {
        ...currMemberInfo,
        name,
        gender,
        birthDate,
        address,
      };
      console.log(
        `Updating member in HealthGorilla for: [patientId: ${attributionInfo.patientId}, mrn: ${attributionInfo.mrn}]`
      );
      return this.client.put(
        PATIENT_ENDPOINT + `/${currMemberInfo.id}`,
        requestBody
      );
    } else {
      console.log(
        `Creating member in HealthGorilla for: [patientId: ${attributionInfo.patientId}, mrn: ${attributionInfo.mrn}]`
      );
      return this.client.post(PATIENT_ENDPOINT, newMemberInfo);
    }
  }

  async uploadCCDA(mrn, ccdBase64) {
    const foundMember = await this.getByIdentifier(mrn, ELATION_DOMAIN);

    if (!foundMember) {
      return;
    }

    console.log("Transforming CCDA request body");
    const ccdBody = this._transformCcdData(foundMember.id, ccdBase64);
    console.log("Sending CCDA");
    return this.client.post(DOCUMENT_ENDPOINT, ccdBody);
  }

  // Private methods
  _transformCcdData(healthGorillaId, ccdBase64) {
    return {
      resourceType: "DocumentReference",
      status: "current",
      type: {
        coding: [
          {
            system: "https://www.healthgorilla.com/document-type",
            code: "CCDDocument",
            display: "Continuity of Care Document",
          },
        ],
        text: "Continuity of Care",
      },
      subject: {
        reference: `Patient/${healthGorillaId}`,
      },
      content: [
        {
          attachment: {
            contentType: "text/xml",
            title: "C-CDA.xml",
            data: ccdBase64,
          },
        },
      ],
    };
  }

  _transformMemberData(attributionInfo) {
    const {
      patientId,
      mrn,
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
    } = attributionInfo;
    const rawGiven = `${firstName}${!!middleName ? " " + middleName : ""}`;
    const text = `${rawGiven} ${lastName}`;
    const given = rawGiven.split(" ").filter(Boolean);
    const suffixRe = /(?<suffix>(?: jr\.?| sr\.?| iv| iii| ii| v|){1,})$/i;
    const rawSuffix = suffixRe.exec(lastName).groups.suffix;
    const name = [
      {
        use: "usual",
        text: text,
        given,
        family: lastName.replace(suffixRe, ""),
        ...(!!rawSuffix && { suffix: rawSuffix.split(" ").filter(Boolean) }),
      },
    ];
    const transformedGender = SEX_MAPPINGS[gender.toLowerCase()] || "unknown";
    const formattedAddressLine2 = addressLine2 ? `, ${addressLine2}` : "";
    const street_address = `${addressLine1}${formattedAddressLine2}`;
    const textAddress = city
      ? `${street_address}, ${city}, ${state} ${zip}`
      : `${street_address}, ${state} ${zip}`;
    const identifier = [
      {
        type: {
          coding: [
            {
              system: "http://hl7.org/fhir/v2/0203",
              code: "MR",
              display: "Medical record number",
            },
          ],
          text: "Medical record number",
        },
        system: CITYBLOCK_DOMAIN,
        value: patientId,
      },
      {
        type: {
          coding: [
            {
              system: "http://hl7.org/fhir/v2/0203",
              code: "MR",
              display: "Medical record number",
            },
          ],
          text: "Medical record number",
        },
        system: ELATION_DOMAIN,
        value: mrn,
      },
    ];
    const address = [
      {
        use: "home",
        type: "both",
        text: textAddress,
        line: street_address,
        city,
        state,
        postalCode: zip,
        country: "us",
      },
    ];

    return {
      resourceType: "Patient",
      active: true,
      identifier,
      birthDate: dob,
      gender: transformedGender,
      name,
      address,
    };
  }

  _validate(attributionInfo) {
    const {
      patientId,
      mrn,
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
        mrn &&
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
        [patientId: ${patientId}], [mrn: ${mrn}], [dob: ${dob}], [gender: ${gender}], [firstName: ${firstName}], [lastName: ${lastName}], [city: ${city}], [state: ${state}], [zip: ${zip}], [addressLine1: ${addressLine1}], [marketId: ${marketId}]`);
    }
  }
}

module.exports = HealthGorillaAPI;
