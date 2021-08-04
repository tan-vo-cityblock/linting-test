import { pick } from 'lodash';
import { NextFunction, Response } from 'express';
import { Model } from 'objection';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';
import { MedicalRecordNumber } from '../models/mrn';

// tslint:disable no-console
export async function getMemberMrnRecords(
  request: IRequestWithTransaction,
  response: Response,
  next: NextFunction,
) {
  try {
    const filter = pick(request.query, ['carrier', 'externalId', 'partner']);

    const memberMrnMapping = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) => {
        return MedicalRecordNumber.getMrnMapping(filter, txn);
      },
    );

    console.log(
      `Returning filtered MRN records [filter: ${JSON.stringify(filter)}, count: ${
        memberMrnMapping.length
      }]`,
    );
    return response.send(memberMrnMapping);
  } catch (e) {
    console.log(`Failed to filter MRN records [error: ${e}]`);
    return next(e);
  }
}
// tslint:enable no-console
