import axios from 'axios';
import base64 from 'base-64';
import dotenv from 'dotenv';
import { compact, head, isEmpty, isNil, map, omitBy, snakeCase } from 'lodash';
import { Transaction } from 'objection';
import { Email, IEmail, IEmailQueryAttributes } from '../models/email';
import { IMemberFilter, Member } from '../models/member';
import { IInsurance, MemberInsurance } from '../models/member-insurance';
import { MemberDemographics } from '../models/member-demographics';
import { IPhone } from '../models/phone';
import { handleAxiosError } from './error-handlers';
dotenv.config();

const zenDeskEndpoint = process.env.ZENDESK_ENDPOINT;
const user = process.env.ZENDESK_USER;
const apiToken = process.env.ZENDESK_API_TOKEN;

const config = {
  headers: {
    'Content-type': 'application/json',
    Authorization: `Basic ${base64.encode(`${user}:${apiToken}`)}`,
  },
};

export interface IZendeskRequest {
  organizationName: string;
  primary_hub: string;
  covid_risk_level?: string;
  care_model: string;
  primary_chp?: string;
  pcp?: string;
}

export interface IZendeskUserCreateRequest {
  name: string;
  external_id: string;
  active: boolean;
  verified: boolean;
  shared?: boolean;
  shared_agent?: boolean;
  locale?: string;
  time_zone?: string;
  email?: string;
  phone?: string;
  details?: string;
  notes?: string;
  organization_id?: number;
  role: string;
  ticket_restriction?: string;
  tags?: string[];
  restricted_agent?: boolean;
  suspended?: boolean;
  user_fields: ICustomUserFields;
}

export interface ICustomUserFields {
  date_of_birth?: string;
  commons_link?: string;
  primary_hub?: string;
  care_model?: string;
  primary_chp?: string;
  pcp?: string;
  covid_risk_level?: string;
  partner_id?: string;
  insurance_carrier?: string;
}

export async function constructZenDeskUser(
  body: IZendeskRequest,
  memberId: string,
  txn: Transaction,
): Promise<IZendeskUserCreateRequest> {
  const { primary_hub, covid_risk_level, care_model, primary_chp, pcp, organizationName } = body;

  const memberDemographics = await MemberDemographics.getByMemberId(memberId, txn);
  const {
    carrier,
    plans,
  }: IInsurance = await MemberInsurance.getLatestInsurance(memberId, txn);

  const organizationNameToIdMapping = {
    'AMBS Answering Service': 370374813593,
    Cityblock: 370240537413,
    Connecticut: 370372249653,
    'Emblem Medicaid Virtual': 370453498174,
    Massachusetts: 370372237373,
    'New York City': 370347849374,
    'Tufts Virtual': 370389559193,
    'Virtual Hub': 370347846154,
    'CareFirst DC': 370481866514,
    'Cityblock Health SANDBOX': 370372011813,
    'North Carolina': 1500414406181,
  };

  const datasourceToCarrierNameMapping = {
    carefirst: 'CareFirst BCBS DC',
  };

  const user_fields: ICustomUserFields = {
    commons_link: `https://commons.cityblock.com/members/${memberId}/360`,
    date_of_birth: memberDemographics.dateOfBirth,
    partner_id: plans[0].externalId,
    primary_hub,
    covid_risk_level,
    care_model,
    primary_chp,
    pcp,
    insurance_carrier: datasourceToCarrierNameMapping[carrier] || carrier,
  };

  const firstEmail = head(memberDemographics.emails.map((e: IEmail) => e.email));
  const formattedEmail = firstEmail && (await formatEmail(firstEmail, memberId, txn));
  const formattedPhone = head(memberDemographics.phones.map((p: IPhone) => formatPhone(p.phone)));
  const formattedName = compact([
    memberDemographics.firstName,
    memberDemographics.middleName && `${memberDemographics.middleName[0]}.`,
    memberDemographics.lastName
  ]).join(' ')

  return omitBy(
    {
      
      name: formattedName,
      external_id: memberId,
      active: true,
      verified: true,
      locale: 'en-US',
      email: formattedEmail,
      phone: formattedPhone,
      tags: compact(map([covid_risk_level, care_model, primary_hub], snakeCase)),
      organization_id: organizationNameToIdMapping[organizationName],
      role: 'end-user',
      ticket_restriction: 'requested',
      user_fields,
    },
    isNil,
  ) as IZendeskUserCreateRequest;
}

export async function createZenDeskUser(body: IZendeskRequest, memberId: string, txn: Transaction) {
  const endpoint: string = `https://${zenDeskEndpoint}/api/v2/users.json`;
  const zenDeskRequest: IZendeskUserCreateRequest = await constructZenDeskUser(body, memberId, txn);
  return axios
    .post(endpoint, { user: zenDeskRequest }, config)
    .then((res) => res.data)
    .catch((error) => handleAxiosError('Zendesk Create', error));
}

export async function deleteZendeskUser(zendeskUserId: string) {
  const endpoint: string = `https://${zenDeskEndpoint}/api/v2/users/${zendeskUserId}`;
  return axios
    .delete(endpoint, config)
    .then((res) => res.data)
    .catch((error) => handleAxiosError('Zendesk Delete', error));
}

export async function formatEmail(email: string, memberId: string, txn: Transaction) {
  // get all email records associated with email in question
  const emailFilter: IEmailQueryAttributes = { email };
  const emailAddresses = await Email.getAllByAttributes(emailFilter, txn);

  // map all members associated with email in question
  const memberIdsFilter: IMemberFilter = { memberIds: emailAddresses.map((e) => e.memberId) };
  const membersForEmail: any = await Member.getMultiple(memberIdsFilter, txn);

  // grab all potential zendesk ids on those members
  const existingZendeskIdsForMembers = membersForEmail.map((m) => m.zendeskId);

  return isEmpty(compact(existingZendeskIdsForMembers)) ? email : null;
}

export function formatPhone(phoneNumber: string) {
  return phoneNumber.startsWith('+1') ? phoneNumber : `+1${phoneNumber}`;
}
