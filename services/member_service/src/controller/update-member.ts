import { NextFunction, Response } from 'express';
import { IMemberUpdate, Member } from '../models/member';
import { updateCityblockMemberInElation } from '../util/elation';
import { IUpdateMemberRequest } from './types';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';


export async function updateMember(
  request: IRequestWithTransaction,
  response: Response,
  next: NextFunction
) {
  const memberId: string = request.params.memberId;
  const {
    demographics,
    insurances,
    zendeskId,
    tags,
    elationPlan,
    updatedBy
  }: IUpdateMemberRequest = request.body;
  const memberUpdate: IMemberUpdate = { demographics, insurances, zendeskId, tags, elationPlan, updatedBy };

  try {
    const elationResponse = await updateCityblockMemberInElation(memberId, memberUpdate);
    if (elationResponse && !elationResponse.success) {
      const failedElationCode = elationResponse.statusCode;
      throw new Error(`[elationStatusCode: ${failedElationCode}, elationResponse: ${elationResponse}]`);
    }
  } catch (e) {
    console.error(`unable to update member in Elation [error: ${e}]`);
    return next(e);
  }
  
  try {
    const { id, cbhId } = await getOrCreateTransaction(
      request.txn,
      Member.knex(),
      async txn => Member.update(memberId, memberUpdate, txn)
    );
    return response.status(200).json({ patientId: id, cityblockId: cbhId });
  } catch (e) {
    console.error(`unable to update member [error: ${e}]`);
    return next(e);
  }
}