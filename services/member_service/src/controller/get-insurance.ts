import { NextFunction, Response } from 'express';
import { Model } from 'objection';
import { memberValidation } from '../middleware/validate-member';
import { MemberInsurance } from '../models/member-insurance';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function getInsurance(
  request: IRequestWithTransaction,
  response: Response,
  next: NextFunction
) {
  
  try {
    // first validate whether the requested member exists
    const { memberId } = request.params;
    const errors = await memberValidation(request, memberId);

    if (errors.length > 0) {
      return response.status(422).json({ error: errors });
    }

    const insurance = await getOrCreateTransaction(request.txn, Model.knex(), async (txn) => {
      return MemberInsurance.getByMember(memberId, txn);
    });
    console.log(`got Insurance Info for {memberId: ${memberId}}`);
    return response.send(insurance);
  } catch (e) {
    console.log(`unable to get insurance information for member: ${e}`);
    return next(e);
  }
}
// tslint:enable no-console
