import { NextFunction, Request, Response } from 'express';
import { transaction } from 'objection';
import { IMemberFilter, Member } from '../models/member';

// tslint:disable no-console
export async function getMembers(
  request: Request,
  response: Response,
  next: NextFunction
) {
  try {
    const filter: IMemberFilter = {
      datasource: request.query.datasource,
      externalIds: request.query.externalIds && request.query.externalIds.split(','),
      partner: request.query.partner,
      current: request.query.current
    };
    console.log(`getting members: filter=${JSON.stringify(filter)}`);
    const members = await transaction(Member.knex(), async txn => {
      return Member.getMultiple(filter, txn);
    });
    const responseBody = { members };
    response.send(responseBody);
  } catch (e) {
    console.log(`error getting members: ${e}`);
    return next(e);
  }
}
// tslint:enable no-console
