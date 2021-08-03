import { Request, Response } from 'express';
import { transaction } from 'objection';
import { parse } from 'qs';
import { IMemberUpdate, Member } from '../models/member';
import { deleteCityblockMemberAttributeInElation } from '../util/elation';

export interface IDeleteMemberAttribute {
  memberId: string;
  tags: string[];
}

// TODO: move these status/sends into error handling middleware
export async function deleteMemberAttribute(request: Request, response: Response) {
  if (!request.params.memberId || !request.query.tags) {
    return response
      .status(400)
      .send(
        `malformed url or query found ; needs tags: [${request.query.tags}] and memberId: [${request.params.memberId}] defined`,
      );
  }
  const deletionAttributes = parse(request.query);
  try {
    const elationResponse = await deleteCityblockMemberAttributeInElation({
      memberId: request.params.memberId,
      ...deletionAttributes,
    });
    if (!elationResponse.success) {
      return response.status(elationResponse.statusCode).json(elationResponse);
    }
    const { id, cbhId } = await transaction(Member.knex(), async (txn) => {
      return Member.update(request.params.memberId, {} as IMemberUpdate, txn);
    });
    return response.status(200).json({ patientId: id, cityblockId: cbhId });
  } catch (e) {
    console.error('error in the controller attribute deletion: ', e);
    return response
      .status(400)
      .send(
        `failed to delete attribute for member: [${request.params.memberId}] with request: [${request}]`,
      );
  }
}
