import axios from 'axios';
import base64 from 'base-64';
import dotenv from 'dotenv';
import { flatten } from 'lodash';
import { compact, isEmpty, isEqual, isNil, omitBy, pick, some, sortBy, union } from 'lodash';
import { transaction } from 'objection';
import querystring from 'querystring';
import { IDeleteMemberAttribute } from '../controller/delete-member-attribute';
import { ICreateMemberRequest } from '../controller/types';
import { IAddress } from '../models/address';
import { IEmail } from '../models/email';
import { IInsuranceDetails } from '../models/member-insurance-details';
import { IMemberUpdate } from '../models/member';
import {  IInsurance, IInsurancePlan, MemberInsurance } from '../models/member-insurance';
import { ELATION, IMedicalRecordNumber, MedicalRecordNumber } from '../models/mrn';
import { IPhone } from '../models/phone';
import { handleAxiosError } from './error-handlers';
dotenv.config();

const elationEndpoint = process.env.ELATION_ENDPOINT;
const grantType = process.env.GRANT_TYPE;
const user = process.env.ELATION_USER;
const pass = process.env.ELATION_PASS;
const clientId = process.env.CLIENT_ID;
const clientSecret = process.env.CLIENT_SECRET;

interface IElationPatient {
  first_name: string;
  last_name: string;
  sex: string;
  dob: string;
  primary_physician: number;
  caregiver_practice: number;
  insurances?: IElationInsurance[];
  address?: IElationAddress;
  phones?: IElationPhone[];
  emails?: IElationEmail[];
  tags?: string[];
}

interface IElationPutRequest {
  first_name: string;
  last_name: string;
  sex: string;
  dob: string;
  primary_physician: number;
  tags?: string[];
}

interface IElationInsurance {
  rank: string;
  carrier: string;
  member_id: string;
  plan?: string | undefined;
}

interface IElationAddress {
  address_line1?: string;
  address_line2?: string;
  city?: string;
  state?: string;
  zip?: string;
}

interface IElationPhone {
  phone?: string;
  phone_type?: string;
}

interface IElationEmail {
  email?: string;
}

interface IElationResponse {
  success: boolean;
  elationId: string;
  reason?: any;
  statusCode?: number;
}

function parseSex(sex: string): string {
  const elationMale = 'Male';
  const elationFemale = 'Female';
  const elationUnknown = 'Unknown';
  let parsedSex: string;
  if (!!sex) {
    switch (sex.toLowerCase()) {
      case 'm':
      case 'male':
        parsedSex = elationMale;
        break;
      case 'f':
      case 'female':
        parsedSex = elationFemale;
        break;
      default:
        parsedSex = elationUnknown;
    }
    return parsedSex;
  } else {
    return undefined;
  }
}

export function parseNewInsurances(
  insurances: IInsurance[],
  elationPlan: string | undefined,
): IElationInsurance[] {
  if (isEmpty(insurances)) {
    return [];
  }
  const elationInsurance: IElationInsurance[][] = insurances.map((insurance) => {
    return insurance.plans
      .filter((plan: IInsurancePlan) => !!plan.current)
      .map((plan: IInsurancePlan) => {
        const latestInsuranceDetails = getLatestInsuranceDetails(plan);
        return parseSingleInsurance(
          insurance.carrier,
          plan.externalId,
          plan.rank,
          latestInsuranceDetails && latestInsuranceDetails.lineOfBusiness,
          elationPlan,
        );
      });
  });
  return flatten(compact(elationInsurance));
}

function getLatestInsuranceDetails(plan: IInsurancePlan): IInsuranceDetails | undefined {
  if (isEmpty(plan.details)) {
    return undefined;
  }    

  const sortedDetails = sortBy(plan.details, (insurance) =>
    new Date(insurance.spanDateStart).getTime(),
  );

  return sortedDetails[sortedDetails.length - 1];
}

// this function is an updated version of the parseInsurance method and should be used instead.
function parseSingleInsurance(
  insurer: string,
  id: string,
  rank: string,
  lineOfBusiness: string | undefined,
  plan: string | undefined,
): IElationInsurance {
  if (some([insurer, id], isEmpty)) {
    return undefined;
  }

  // elation maps connecticare as two different insurances whereas the member index does not
  const updatedInsurer = parseInsurer(insurer, lineOfBusiness);
  const updatedRank = !!rank ? rank : 'primary';

  // `carrier` values for insurance providers on Elation instance
  const insurerToElationName = {
    bcbsNC: 'Blue Cross and Blue Shield of North Carolina',
    medicaidNC: 'Medicaid of NC',
    medicareNC: 'Medicare of NC',
    connecticaremedicare: 'Connecticare-VIP Medicare Plans',
    connecticarecommercial: 'Connecticare, Inc.',
    selfPay: 'selfPay',
    carefirst: 'CareFirst BlueCross BlueShield Community Health Plan District of Columbia',
    tufts: 'Tufts Health Plan',
    emblem: 'Emblem Health',
    cardinal: 'Cardinal Innovations',
    "cardinal (dual)": 'Cardinal Innovations',
    "cardinal innovations": 'Cardinal Innovations',
    healthyblue: 'Healthy Blue',
  };
  // only handling a single insurance at this time
  return { rank: updatedRank, carrier: insurerToElationName[updatedInsurer], member_id: id, plan };
}

function parseInsurer(insurer: string, lineOfBusiness: string): string {
  return insurer === 'connecticare' && !!lineOfBusiness
    ? `${insurer}${lineOfBusiness.toLowerCase()}`
    : insurer;
}

export function parseAddresses(addresses: IAddress[]): IElationAddress {
  // The elation API only takes in one address, so as of now, we take the first address only
  if (isEmpty(addresses)) {
    return undefined;
  }
  return {
    address_line1: addresses[0].street1,
    address_line2: addresses[0].street2,
    city: addresses[0].city,
    state: addresses[0].state,
    zip: addresses[0].zip,
  };
}

export function parsePhones(phones: IPhone[]): IElationPhone[] {
  return (
    phones &&
    phones.map((phone) => {
      return {
        phone: phone.phone,
        phone_type: 'Home',
      };
    })
  );
}

function parseEmails(emails: IEmail[]): IElationEmail[] {
  return (
    emails &&
    emails.map((email) => {
      return {
        email: email.email,
      };
    })
  );
}

/* 
now since we will need to create emblem virtual members in elation, we should explicitly ask for this in the short term 
until we build out a better abstraction to load members.
*/
export function isElationCallNecessary(requestBody: ICreateMemberRequest) {
  return (
    process.env.NODE_ENV !== 'development' &&
    (requestBody.createElationPatient === true || requestBody.createElationPatient === undefined)
  );
}

export function parseTags(requestBody: ICreateMemberRequest | IMemberUpdate): string[] {
  if (Array.isArray(requestBody.tags)) {
    return requestBody.tags;
  } else if (!!requestBody.tags) {
    return [requestBody.tags];
  }
  return undefined;
}

async function ensureUniqueTags(
  request: IElationPutRequest,
  elationId: string,
): Promise<IElationPutRequest> {
  const currentPatientState = await getPatientInElation(elationId);
  const putRequest = {
    first_name: currentPatientState.first_name,
    last_name: currentPatientState.last_name,
    dob: currentPatientState.dob,
    sex: currentPatientState.sex,
    caregiver_practice: currentPatientState.caregiver_practice,
    primary_physician: currentPatientState.primary_physician,
  };
  request.tags = union(request.tags, currentPatientState.tags);
  if (!request.tags.length) {
    request.tags = undefined;
  }
  return { ...putRequest, ...request };
}

function createElationPatient(requestBody: ICreateMemberRequest): IElationPatient {
  const insurances: IElationInsurance[] = parseNewInsurances(
    requestBody.insurances,
    requestBody.elationPlan,
  );

  const first_name = requestBody.demographics.firstName;
  const last_name = requestBody.demographics.lastName;
  const dob = requestBody.demographics.dateOfBirth;
  const sex = parseSex(requestBody.demographics.sex);
  const address = parseAddresses(requestBody.demographics.addresses);
  const phones = parsePhones(requestBody.demographics.phones);
  const emails = parseEmails(requestBody.demographics.emails);

  return {
    first_name,
    last_name,
    dob,
    sex,
    primary_physician: requestBody.primaryPhysician,
    caregiver_practice: requestBody.caregiverPractice,
    insurances,
    address,
    phones,
    emails,
    tags: parseTags(requestBody),
  };
}

function createElationDeleteAttributePatient(request: IDeleteMemberAttribute): IElationPutRequest {
  const cleanRequest: IDeleteMemberAttribute = omitBy(request, isNil);
  if (isEmpty(cleanRequest)) {
    return undefined;
  } else {
    return omitBy(
      {
        tags: parseTags(cleanRequest),
      },
      isNil,
    );
  }
}

function createElationUpdatePatient(request: IMemberUpdate): IElationPutRequest {
  const cleanRequest = omitBy(request, isNil);
  // this assumes all values in request can be passed to Elation
  if (isEmpty(cleanRequest)) {
    return undefined;
  } else {
    const insurances: IElationInsurance[] = parseNewInsurances(
      cleanRequest.insurances,
      cleanRequest.elationPlan,
    );
    const { firstName, lastName, dateOfBirth, sex, addresses, phones, emails } =
      cleanRequest.demographics || {};
    return omitBy(
      {
        first_name: firstName,
        last_name: lastName,
        dob: dateOfBirth,
        sex: parseSex(sex),
        address: parseAddresses(addresses),
        phones: parsePhones(phones),
        emails: parseEmails(emails),
        tags: parseTags(cleanRequest),
        insurances,
      },
      isNil,
    );
  }
}

async function getElationAuthToken() {
  const tokenUrl = `https://${elationEndpoint}/api/2.0/oauth2/token/`;
  const headers = {
    'Content-type': 'application/x-www-form-urlencoded',
    Authorization: `Basic ${base64.encode(`${clientId}:${clientSecret}`)}`,
  };

  return axios
    .post(
      tokenUrl,
      querystring.stringify({ grant_type: grantType, username: user, password: pass }),
      {
        headers,
      },
    )
    .then((res) => res.data.access_token)
    .catch((error) => handleAxiosError('Elation', error));
}

export async function createPatientInElation(requestBody: ICreateMemberRequest): Promise<any> {
  const elationRequest = createElationPatient(requestBody);
  const elationAuthToken = await getElationAuthToken();
  const headers = {
    'Content-type': 'application/json',
    Authorization: `Bearer ${elationAuthToken}`,
  };
  return axios
    .post(`https://${elationEndpoint}/api/2.0/patients/`, elationRequest, {
      headers,
    })
    .then((r) => r.data)
    .catch((error) => handleAxiosError('Elation', error));
}

// tslint:disable no-console
export async function deletePatientInElation(elationId: string): Promise<IElationResponse> {
  console.log(`going to delete elation member: ${elationId}`);
  const elationAuthToken = await getElationAuthToken();
  return axios
    .delete(`https://${elationEndpoint}/api/2.0/patients/${elationId}`, {
      headers: { Authorization: `Bearer ${elationAuthToken}` },
    })
    .then((r) => ({ success: r.status === 204, elationId }))
    .catch((error) => {
      console.error(`delete patient in elation error for ${elationId}: `, error.response.data);
      return { success: false, elationId };
    });
}
// tslint:enable no-console

export async function deletePatientIfElation(
  mrn: IMedicalRecordNumber | null,
): Promise<IElationResponse> {
  if (!!mrn && mrn.name === ELATION) {
    return deletePatientInElation(mrn.id);
  }
  return Promise.resolve(null);
}

// tslint:disable no-console
export async function deleteCityblockMemberAttributeInElation(
  requestParams: IDeleteMemberAttribute,
) {
  // TODO: if the member is an emblem member, this function will report that it failed to find the member in elation
  const elationRequest = createElationDeleteAttributePatient(requestParams);
  const elationMDI = await transaction(MemberInsurance.knex(), async (txn) => {
    return MedicalRecordNumber.getMrn(requestParams.memberId, txn);
  });
  if (!elationMDI) {
    console.log(`elation MDIs not found for member: ${requestParams.memberId}`);
    return;
  }
  const elationId = elationMDI.id;
  if (isEmpty(elationRequest)) {
    console.log(`nothing to delete for member: ${requestParams.memberId}`);
    return {
      success: true,
      elationId,
      statusCode: 304,
    };
  }
  const currentPatientState = await getPatientInElation(elationId);
  const cleanRequestBody = await safeDeleteListElementsElationPatient(
    elationRequest,
    currentPatientState,
  );
  if (
    isEqual(
      cleanRequestBody,
      pick(currentPatientState, [
        'first_name',
        'last_name',
        'sex',
        'dob',
        'primary_physician',
        'caregiver_practice',
      ]),
    )
  ) {
    console.log(`nothing to delete for member: ${requestParams.memberId}`);
    return {
      success: true,
      elationId,
      statusCode: 304,
    };
  }
  return deletePatientAttributeInElation(elationId, cleanRequestBody);
}
// tslint:enable no-console

// tslint:disable no-console
async function deletePatientAttributeInElation(
  elationId: string,
  requestBody: IElationPutRequest,
): Promise<IElationResponse> {
  console.log(
    `going to delete attributes: [${Object.keys(requestBody)}] from elation member: ${elationId}`,
  );
  const elationAuthToken = await getElationAuthToken();
  const headers = {
    'Content-type': 'application/json',
    Authorization: `Bearer ${elationAuthToken}`,
  };
  return axios
    .put(`https://${elationEndpoint}/api/2.0/patients/${elationId}`, requestBody, { headers })
    .then((r) => {
      console.log(`deletion of item(s) for patient in elation complete for patient ${elationId}`);
      return {
        success: r.status === 200,
        elationId,
        statusCode: r.status,
      };
    })
    .catch((error) => {
      console.error(
        `deletion of items(s) for patient in elation error for ${elationId}: `,
        error.response.data,
      );
      return {
        success: false,
        elationId,
        reason: error.response.data,
        statusCode: error.response.status,
      };
    });
}
// tslint:enable no-console

// returns set difference between requestBody, currentPatientState for submission as a put call
async function safeDeleteListElementsElationPatient(
  requestBody: IElationPutRequest,
  currentPatientState: IElationPatient,
): Promise<IElationPutRequest> {
  const putRequest = {
    first_name: currentPatientState.first_name,
    last_name: currentPatientState.last_name,
    sex: currentPatientState.sex,
    dob: currentPatientState.dob,
    primary_physician: currentPatientState.primary_physician,
    caregiver_practice: currentPatientState.caregiver_practice,
  };
  if (isEmpty(requestBody)) {
    return putRequest;
  }
  Object.keys(requestBody).forEach((memberAttribute) => {
    if (!currentPatientState[memberAttribute]) {
      requestBody[memberAttribute] = undefined;
    } else if (Array.isArray(requestBody[memberAttribute])) {
      requestBody[memberAttribute] = currentPatientState[memberAttribute].filter(
        (memberValue) => !requestBody[memberAttribute].includes(memberValue),
      );
    } else if (requestBody[memberAttribute] === currentPatientState[memberAttribute]) {
      requestBody[memberAttribute] = null;
    } else {
      requestBody[memberAttribute] = undefined;
    }
  });
  return omitBy({ ...putRequest, ...requestBody }, isNil);
}

// tslint:disable no-console
async function getPatientInElation(elationId: string): Promise<IElationPatient> {
  const elationAuthToken = await getElationAuthToken();
  const headers = {
    Authorization: `Bearer ${elationAuthToken}`,
  };
  console.log(`going to get elation member: ${elationId}`);
  return axios
    .get(`https://${elationEndpoint}/api/2.0/patients/${elationId}`, {
      headers,
    })
    .then((r) => r.data)
    .catch((error) => handleAxiosError('Elation', error));
}
// tslint:enable no-console

// tslint:disable no-console
async function updatePatientInElation(
  elationId: string,
  elationRequest: any,
): Promise<IElationResponse> {
  console.log(`Updating member in Elation [elationId: ${elationId}, elationRequest: ${elationRequest}]`);
  const elationAuthToken = await getElationAuthToken();
  return axios
    .put(`https://${elationEndpoint}/api/2.0/patients/${elationId}`, elationRequest, {
      headers: { Authorization: `Bearer ${elationAuthToken}` },
    })
    .then((r) => {
      console.log(`Elation member update complete: [elationId: ${elationId}]`);
      return { success: r.status === 200, elationId, statusCode: r.status };
    })
    .catch((error) => {
      console.error(`Failure to update member in Elation [elationId: ${elationId}, error: `, error.response.data, ']');
      return {
        success: false,
        elationId,
        reason: error.response.data,
        statusCode: error.response.status,
      };
    });
}
// tslint:enable no-console

// tslint:disable no-console
// TODO: turn into a middleware function
export async function updateCityblockMemberInElation(
  memberId: string,
  memberUpdate: IMemberUpdate
): Promise<IElationResponse> {
  const elationRequest = createElationUpdatePatient(memberUpdate);
  if (isEmpty(elationRequest)) {
    console.log(`Nothing to update for member in Elation [memberId: ${memberId}]`);
    return;
  }
  const memberMrn = await transaction(MedicalRecordNumber.knex(), async (txn) => {
    return MedicalRecordNumber.getMrn(memberId, txn);
  });

  if (memberMrn && memberMrn.name === 'elation') {
    const elationId = memberMrn.id;
    const elationRequestUniqueTags = await ensureUniqueTags(elationRequest, elationId);
    return updatePatientInElation(elationId, elationRequestUniqueTags);
  } else {
    return Promise.resolve(null);
  }
}
// tslint:enable no-console
