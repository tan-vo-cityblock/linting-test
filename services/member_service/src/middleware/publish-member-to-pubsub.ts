import crypto from 'crypto';
import { filter, flatMap, head, includes, map, orderBy } from 'lodash';
import { Model, Transaction } from 'objection';
import { Attributes, PubSub } from '@google-cloud/pubsub';
import { ICreateAndPublishMemberRequest, IMemberPubSubMessage } from '../controller/types';
import { getOrCreateTransaction, IRequestWithPubSub } from '../controller/utils';
import { IAddress } from '../models/address';
import { Cohort } from '../models/cohort';
import { IEmail } from '../models/email';
import {
  IInsurancePlan,
  IInsurance,
  MemberInsurance,
  STATE_IDS,
} from '../models/member-insurance';
import { IInsuranceDetails } from '../models/member-insurance-details';
import { IPhone } from '../models/phone';

// parse env vars
const pubsubProjectId = process.env.PUBSUB_GCP_PROJECT_ID;
const pubsubTopic = process.env.PUBSUB_TOPIC; // should be something like memberAttribution
const hmacSecret = process.env.PUBSUB_HMAC_SECRET;

// create pubsub client
const fullTopicName = `projects/${pubsubProjectId}/topics/${pubsubTopic}`;
const pubsubInstance = new PubSub({ projectId: pubsubProjectId });

// TODO: update to use relevant IInsurances-like type
interface IMemberInsurance {
  carrier: string;
  externalId: string;
  rank?: string;
  lineOfBusiness?: string;
  subLineOfBusiness?: string;
  spanDateStart?: Date;
  spanDateEnd?: Date;
  current?: boolean;
}

export interface IdsForAttributionUpdate {
  patientId: string;
  mrnId?: string;
  cityblockId?: number;
}

export interface IdsForAttributionCreate {
  patientId: string;
  mrnId: string;
  cityblockId: number;
}

// tslint:disable no-console
export async function publishMemberToPubsub(
  request: IRequestWithPubSub,
  idsForAttribution: IdsForAttributionCreate | IdsForAttributionUpdate,
): Promise<string> {
  const { patientId } = idsForAttribution;

  try {
    const publishMemberRequest: IMemberPubSubMessage = await requestToMemberPubSubMessage(
      request.body,
      idsForAttribution,
      request.txn
    );

    // create data buffer and message attributes
    const data = stringifyMessage(publishMemberRequest);
    const dataBuffer = Buffer.from(data);
    const _hmacSecret_ = request.hmacSecret || hmacSecret;
    const msgAttributes: Attributes = {
      topic: pubsubTopic,
      hmac: signPayload(data, _hmacSecret_),
    };

    // send message through pubsub
    const pubsub = request.pubsub || pubsubInstance;
    const topicName = request.topicName || fullTopicName;
    const messageId = await pubsub.topic(topicName).publish(dataBuffer, msgAttributes);

    console.log(`Message ${messageId} published [memberId: ${patientId}, topic: ${topicName}]`);
    console.log(`Message contents [data: ${JSON.stringify(data)}]`);

    return messageId;
  } catch (err) {
    return Promise.reject(err);
  }
}
// tslint:enable no-consoles

export const signPayload = (payload: string, _hmacSecret_: string): string => {
  return crypto.createHmac('SHA256', _hmacSecret_).update(payload).digest('hex');
};

export function stringifyMessage(
  request: Object,
): string {
  return JSON.stringify(request).replace(/\s\s+/g, ' ');
};

export async function requestToMemberPubSubMessage(
  memberRequest: ICreateAndPublishMemberRequest,
  idsForAttribution: IdsForAttributionCreate | IdsForAttributionUpdate,
  testTxn: Transaction
): Promise<IMemberPubSubMessage> {
  const { patientId, cityblockId, mrnId } = idsForAttribution;
  const {
    pcpAddress,
    pcpName,
    pcpPhone,
    pcpPractice,
    marketId,
    partnerId,
    clinicId,
    medicaid,
    medicare,
  } = memberRequest;

  const cohort: Cohort = await getOrCreateTransaction(
    testTxn,
    Model.knex(),
    async (txn) => Cohort.getById(memberRequest.cohortId, txn)
  );

  const cohortName = (cohort && cohort.name) ? cohort.name : undefined;

  const latestInsurance: IInsurance = await getOrCreateTransaction(
    testTxn,
    Model.knex(),
    async (txn) => MemberInsurance.getLatestInsurance(patientId, txn)
  );

  // TODO: this whole section is terrible and awful but needed :pray:
  // we need to refactor such that we can guarantee there is always a current id!
  let activeInsurance: IMemberInsurance;
  if (!!latestInsurance) {
    const firstPlan: IInsurancePlan = head(latestInsurance.plans);
    const firstPlanDetails: IInsuranceDetails =
      firstPlan.details && head(orderBy(firstPlan.details, ['spanDateEnd', 'spanDateStart'], ['desc', 'desc']));
    activeInsurance = {
      carrier: latestInsurance.carrier,
      externalId: firstPlan.externalId,
      current: firstPlan.current,
      rank: firstPlan.rank,
      lineOfBusiness: firstPlanDetails && firstPlanDetails.lineOfBusiness,
      subLineOfBusiness: firstPlanDetails && firstPlanDetails.subLineOfBusiness
    };
  } else {
    const insurances: IMemberInsurance[] = flatMap(
      memberRequest.insurances,
      (i: IInsurance) =>
        flatMap(i.plans, (plan: IInsurancePlan) => {
          return map(plan.details, (detail: IInsuranceDetails) => {
            return { carrier: i.carrier, ...plan, ...detail };
          });
        }),
    );
    activeInsurance =
      head(filter(insurances, (i: IMemberInsurance) => !includes(i.carrier, STATE_IDS))) || {};
  }

  // TODO: medicare and medicaid ids should be pulled as latest ids from database instead of ids on request
  const medicaidId = medicaid && medicaid[0] && medicaid[0].plans && medicaid[0].plans[0] && medicaid[0].plans[0].externalId;
  const medicareId = medicare && medicare[0] && medicare[0].plans && medicare[0].plans[0] && medicare[0].plans[0].externalId;

  const insurance: string = activeInsurance && activeInsurance.carrier;
  const memberId: string = activeInsurance && activeInsurance.externalId;
  const lineOfBusiness: string = activeInsurance && activeInsurance.lineOfBusiness;
  const productDescription: string = activeInsurance && activeInsurance.subLineOfBusiness;

  const {
    firstName,
    middleName,
    lastName,
    sex: gender,
    dateOfBirth: dob,
    dateOfDemise: dod,
    ethnicity,
    race,
    language,
    maritalStatus,
    addresses,
    phones,
    emails,
    ssn,
    shouldUpdateAddress,
  } = memberRequest.demographics || {};

  const address: IAddress = head(addresses);
  const phoneNumber: IPhone = head(phones);
  const emailAddress: IEmail = head(emails);

  const addressLine1 = address && address.street1;
  const addressLine2 = address && address.street2;
  const city = address && address.city;
  const county = address && address.county;
  const state = address && address.state;
  const zip = address && address.zip;
  const phone = phoneNumber && phoneNumber.phone;
  const email = emailAddress && emailAddress.email;

  return {
    patientId,
    cityblockId,
    memberId,
    nmi: memberId,
    mrn: mrnId,
    marketId,
    partnerId,
    clinicId,
    pcpAddress,
    pcpName,
    pcpPhone,
    pcpPractice,

    // demographic fields
    firstName,
    middleName,
    lastName,
    gender,
    shouldUpdateAddress,
    addressLine1,
    addressLine2,
    city,
    county,
    state,
    zip,
    ssn,
    dob,
    dod,
    email,
    phone,
    ethnicity,
    race,
    language,
    maritalStatus,
    cohortName,
    
    // insurance fields
    insurance,
    lineOfBusiness,
    medicareId,
    medicaidId,
    productDescription,
  } as IMemberPubSubMessage;
}
