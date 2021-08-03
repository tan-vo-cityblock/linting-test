import { NextFunction, Request, Response } from 'express';
import { transaction } from 'objection';
import { MemberInsurance } from '../models/member-insurance';

// tslint:disable no-console
export async function deleteInsurance(request: Request, response: Response, next: NextFunction) {
  try {
    const { memberId, insuranceId } = request.params;
    const { deletedReason } = request.body;
    console.log(`Deleting insurance id [memberId: ${memberId}, insuranceId: ${insuranceId}]`);
    const deletedInsurance = await transaction(MemberInsurance.knex(), async (txn) => {
      return MemberInsurance.deleteByInsurance(memberId, insuranceId, deletedReason, txn);
    });
    if (deletedInsurance) {
      response.send({
        deleted: true,
        memberId: deletedInsurance.memberId,
        insuranceId: deletedInsurance.externalId,
        deletedAt: deletedInsurance.deletedAt,
        deletedReason: deletedInsurance.deletedReason,
      });
    } else {
      response
        .status(422)
        .send(`Member ${memberId} does not exist with insurance ID ${insuranceId}`);
    }
  } catch (e) {
    console.log('error in the insurance controller delete', e);
    return next(e);
  }
}
// tslint:enable no-console
