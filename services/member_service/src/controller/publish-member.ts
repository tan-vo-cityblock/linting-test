import { NextFunction, Response } from 'express';
import { Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { publishMemberToPubsub, IdsForAttributionUpdate } from '../middleware/publish-member-to-pubsub';
import { IMemberUpdate, Member } from '../models/member';
import { IMedicalRecordNumber, MedicalRecordNumber } from '../models/mrn';
import { ICreateAndPublishMemberRequest } from './types';
import { getOrCreateTransaction, IRequestWithPubSub } from './utils';

// tslint:disable no-console
export async function updateAndPublishMember(
  request: IRequestWithPubSub,
  response: Response,
  next: NextFunction
) {
  // get member info from request
  let updateResult: { member: Member, mrn: IMedicalRecordNumber };
  const { memberId } = request.params;
  const memberRequest: ICreateAndPublishMemberRequest = request.body;

  try {
    updateResult = await getOrCreateTransaction(request.txn, Model.knex(), async (txn) => {
      const member = await updateMemberDataOnAttribution(memberId, memberRequest, txn);
      const mrn = await MedicalRecordNumber.getMrn(memberId, txn);
      return { member, mrn };
    });
  } catch (e) {
    console.error(`error updating member index on attribution [error: ${e}]`);
    return next(e);
  }

  try {
    const { mrn, member } = updateResult;
    // create data buffer and message attributes
    const idsForAttribution: IdsForAttributionUpdate = {
      patientId: memberId,
      cityblockId: member.cbhId,
      mrnId: mrn && mrn.id
    };

    const messageId: string = await publishMemberToPubsub(request, idsForAttribution);

    const responseBody = {
      patientId: memberId,
      cityblockId: member.cbhId,
      mrn: mrn && mrn.id,
      messageId,
    };

    return response.send(responseBody);
  } catch (e) {
    console.error(`error publishing attribution message to Pub/Sub [memberId: ${memberId}, error: ${e}]`);
    return next(e);
  }
};
// tslint:enable no-consoles

// TODO: tie this up so that updateMemberDataOnAttribution uses Member.update to fully update member demo/insurance
async function updateMemberDataOnAttribution(
  memberId: string,
  memberRequest: ICreateAndPublishMemberRequest,
  txn: Transaction,
): Promise<Member> {
  if (!validate(memberId)) {
    throw new Error(`invalid memberId on member attribution update [memberId: ${memberId}]`);
  }
  const member: Member = await Member.get(memberId, txn);
  if (!member) {
    throw new Error(`member does not exist on member attribution update [memberId: ${memberId}]`);
  }

  const memberUpdate: IMemberUpdate = {
    demographics: memberRequest.demographics,
    insurances: memberRequest.insurances
  };

  return Member.update(member.id, memberUpdate, txn);
}
