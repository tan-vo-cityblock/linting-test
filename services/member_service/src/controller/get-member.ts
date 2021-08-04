import { NextFunction, Request, Response } from 'express';
import { transaction } from 'objection';
import { Member } from '../models/member';

// tslint:disable no-console
export async function getMember(
  request: Request,
  response: Response,
  next: NextFunction
) {
  console.log(`getting member [memberId: ${request.params.memberId}]`);
  try {
    const member = await transaction(Member.knex(), async txn => {
      return Member.get(request.params.memberId, txn);
    });
    console.log(`returned member [memberId: ${member.id}]`);
    response.send(member);
  } catch (e) {
    console.log(`error getting member: ${e}`);
    return next(e);
  }
}

export async function getMemberByZendeskId(
  request: Request,
  response: Response,
  next: NextFunction
) {
  const zendeskId = request.params.zendeskId;
  console.log(`getting member by zendesk id [zendeskId: ${zendeskId}]`);

  try {
    const member = await transaction(Member.knex(), async txn => {
      return Member.getByZendeskId(zendeskId, txn);
    });
    console.log(`returned member by zendesk id [zendeskId: ${zendeskId}, memberId: ${member.id}]`);
    response.send(member);
  } catch (e) {
    console.log(`error getting member by zendeskId: ${e}`);
    return next(e);
  }
}
// tslint:enable no-console
