import { pick } from 'lodash';
import { NextFunction, Response } from 'express';
import { Model } from 'objection';
import { MemberInsurance, MemberInsuranceMapping } from '../models/member-insurance';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function getMemberInsuranceRecords(
  request: IRequestWithTransaction,
  response: Response,
  next: NextFunction,
) {
  try {
    const filter = pick(request.query, ['carrier', 'externalId', 'partner']);

    const memberInsuranceMapping: MemberInsuranceMapping[] = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) =>  MemberInsurance.getMemberInsuranceMapping(filter, txn)
    );

    console.log(
      `Returning filtered member insurance records [filter: ${JSON.stringify(filter)}, count: ${memberInsuranceMapping.length}]`,
    );
    return response.send(memberInsuranceMapping);
  } catch (e) {
    console.log(`Failed to filter member insurance records [error: ${e}]`);
    return next(e);
  }
}
// tslint:enable no-console
