import { Response } from 'express';
import { values } from 'lodash';
import { Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import {
  IUpdateMemberDemographics,
  MaritalStatus,
  MemberDemographics,
  Sex,
} from '../models/member-demographics';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function updateMemberDemographics(
  request: IRequestWithTransaction,
  response: Response,
) {
  const updatedMemberDemographics: IUpdateMemberDemographics = request.body;
  const { memberId } = request.params;

  try {
    const memberDemographics = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) => {
        const errors = await requestValidation(memberId, updatedMemberDemographics, txn);
        if (errors.length > 0) throw new Error(errors.toString());
        return MemberDemographics.updateAndSaveHistory(memberId, updatedMemberDemographics, txn);
      },
    );

    return response.send(memberDemographics);
  } catch (e) {
    const failedRequirement = `unable to update demographics for [member: ${memberId}]`;
    console.log('unable to update demographics for member', e);
    return response.status(422).json({ error: failedRequirement, body: e });
  }
}
// tslint:enable no-console

// tslint:disable no-console
async function requestValidation(
  memberId: string,
  updatedMemberDemographics: IUpdateMemberDemographics,
  txn: Transaction,
) {
  const { sex, maritalStatus } = updatedMemberDemographics;

  const errorBody = [];
  if (sex && !values(Sex).includes(sex)) {
    const enums = values(Sex).join(', ');
    const failedRequirement = `sex must be one of { ${enums} } [sex: ${sex}]`;
    console.error(`unable to update demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  if (maritalStatus && !values(MaritalStatus).includes(maritalStatus)) {
    const enums = values(MaritalStatus).join(', ');
    const failedRequirement = `maritalStatus must be one of { ${enums} } [maritalStatus: ${maritalStatus}]`;
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
