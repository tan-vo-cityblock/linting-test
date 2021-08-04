import { Response } from 'express';
import { compact, isNil, some, values } from 'lodash';
import { Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { Address } from '../models/address';
import { Email } from '../models/email';
import { Member } from '../models/member';
import { Phone } from '../models/phone';
import { IAppendMemberDemographicsRequest } from './types';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function appendMemberDemographics(
  request: IRequestWithTransaction,
  response: Response,
) {
  const appendedMemberDemographics: IAppendMemberDemographicsRequest = request.body;
  const { emails, addresses, phones, updatedBy } = appendedMemberDemographics;
  const { memberId } = request.params;

  try {
    await getOrCreateTransaction(request.txn, Model.knex(), async (txn) => {
      const errors = await requestValidation(memberId, appendedMemberDemographics, txn);
      if (errors.length > 0) throw new Error(errors.toString());
      const updateEmails = Email.create(memberId, emails, txn);
      const updatePhones = Phone.create(memberId, phones, txn);
      const updateAddresses = Address.create(memberId, addresses, txn);
      return Promise.all([updateEmails, updatePhones, updateAddresses]);
    });

    const appends: string[] = [];
    if (!isNil(emails)) appends.push(`emails: ${emails.map((e) => e.id)}`);
    if (!isNil(phones)) appends.push(`phones: ${phones.map((p) => p.id)}`);
    if (!isNil(addresses)) appends.push(`addresses: ${addresses.map((a) => a.id)}`);
    console.log(
      `appended to member demographics [member: ${memberId}, updatedBy: ${updatedBy}, ${appends.join(
        ', ',
      )}]`,
    );

    return response.send({ memberId });
  } catch (err) {
    const failedRequirement = `unable to append demographics [member: ${memberId}, updatedBy: ${updatedBy}]`;
    console.log('unable to append demographics for member', err);
    return response.status(422).json({ error: failedRequirement, body: err });
  }
}
// tslint:enable no-console

// tslint:disable no-console
async function requestValidation(
  memberId: string,
  appendedMemberDemographics: IAppendMemberDemographicsRequest,
  txn: Transaction,
) {
  const { phones, addresses, emails } = appendedMemberDemographics;

  const errorBody = [];

  // make sure that each phone object has an actual phone number associated with it
  if (phones && some(phones, (phoneObj) => !phoneObj.phone)) {
    const failedRequirement = `at least one phone sent without a number [phones: ${phones}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  // make sure that each email object has an actual email address associated with it
  if (emails && some(emails, (emailObj) => !emailObj.email)) {
    const failedRequirement = `at least one email sent without an email address [emails: ${emails}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  // make sure that each address has at least one non empty attribute
  if (addresses && some(addresses, (address) => compact(values(address)).length === 0)) {
    const failedRequirement = `at least one address sent without any content [addresses: ${addresses}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  if (!validate(memberId)) {
    const failedRequirement = `invalid UUID [memberId: ${memberId}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);

  if (!member) {
    const failedRequirement = `member does not exist [memberId: ${memberId}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  return errorBody;
}
// tslint:enable no-console
