import { Response } from 'express';
import { Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import { MemberDemographics } from '../models/member-demographics';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function getMemberDemographics(request: IRequestWithTransaction, response: Response) {
  const { memberId } = request.params;

  try {
    const memberDemographics = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) => {
        const errors = await requestValidation(memberId, txn);
        if (errors.length > 0) throw new Error(errors.toString());
        return MemberDemographics.getByMemberId(memberId, txn);
      },
    );
    console.log(`getRequest to member demographics [member: ${memberId}, `);

    return response.send(memberDemographics);
  } catch (err) {
    const failedRequirement = `unable to fetch demographics [member: ${memberId}]`;
    console.log('unable to process request', err);
    return response.status(422).json({ error: failedRequirement, body: err });
  }
}
// tslint:enable no-console

// tslint:disable no-console
async function requestValidation(memberId: string, txn: Transaction) {
  const errorBody = [];

  if (!validate(memberId)) {
    const failedRequirement = `invalid UUID [memberId: ${memberId}]`;
    console.error(`unable to fetch demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);

  if (!member) {
    const failedRequirement = `member does not exist [memberId: ${memberId}]`;
    console.error(`unable to fetch demographics for member: ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }

  return errorBody;
}
// tslint:enable no-console
