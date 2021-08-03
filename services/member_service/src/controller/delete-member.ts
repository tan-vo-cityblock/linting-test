import { NextFunction, Request, Response } from 'express';
import { transaction, Model } from 'objection';
import { Member } from '../models/member';

// tslint:disable no-console
export async function deleteMember(request: Request, response: Response, next: NextFunction) {
  try {
    const member = await transaction(Member.knex(), async (txn) => {
      const { memberId } = request.params;
      const { reason, deletedBy } = request.query;
      console.log(`deleting member: ${memberId} by user: ${deletedBy}`);
      return Member.delete(memberId, reason, deletedBy, txn);
    });
    response.send({
      deleted: true,
      deletedAt: member.deletedAt,
      deletedReason: member.deletedReason,
      deletedBy: member.deletedBy,
    });
  } catch (e) {
    console.log(`error deleting member: ${e}`);
    return next(e);
  }
}

export async function deleteMemberMrn(request: Request, response: Response, next: NextFunction) {
  try {
    await transaction(Model.knex(), async (txn) => {
      const { memberId } = request.params;
      const { reason, deletedBy } = request.query;
      console.log(`deleting mrn info from member [member: ${memberId}, user: ${deletedBy}]`);
      return Member.deleteMemberMrn(memberId, reason, deletedBy, txn);
    });
    response.send({ deleted: true });
  } catch (e) {
    console.log(`error deleting member mrn: ${e}`);
    return next(e);
  }
}
// tslint:enable no-console
