import { NextFunction, Response } from 'express';
import { compact } from 'lodash';
import { Model } from 'objection';
import { publishMemberToPubsub, IdsForAttributionUpdate } from '../middleware/publish-member-to-pubsub';
import { memberValidation } from '../middleware/validate-member';
import { Member } from '../models/member';
import { IInsurance, MemberInsurance } from '../models/member-insurance';
import { IMedicalRecordNumber, MedicalRecordNumber } from '../models/mrn';
import { updateCityblockMemberInElation } from '../util/elation';
import { getOrCreateTransaction, IRequestWithPubSub } from './utils';

export async function resyncMember(
  request: IRequestWithPubSub,
  response: Response,
  next: NextFunction
) {
  const memberId = request.params.memberId;
  const errors = await memberValidation(request, memberId);

  if (errors.length > 0) {
    return next({ error: errors });
  }

  try {
    // get latest insurance
    const { latestMemberInsurance, mrnId, cohortId } = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) => {
        const latestMemberInsurance: IInsurance = await MemberInsurance.getLatestInsurance(memberId, txn);
        const mrn: IMedicalRecordNumber = await MedicalRecordNumber.getMrn(memberId, txn);
        const member: Member = await Member.get(memberId, txn);
        return { latestMemberInsurance, mrnId: mrn && mrn.id, cohortId: member.cohortId };
      }
    );

    const insuranceUpdate = { insurances: compact([latestMemberInsurance]) };
    
    // update elation
    const elationResponse = await updateCityblockMemberInElation(memberId, insuranceUpdate);
    console.log(`Updated Elation with latest insurances [memberId: ${memberId}, insuranceUpdate: ${JSON.stringify(insuranceUpdate)}]`);
    if (elationResponse && !elationResponse.success) {
      return response.status(elationResponse.statusCode).json(elationResponse);
    }

    // send to commons
    try {
      const idsForAttribution: IdsForAttributionUpdate = { patientId: memberId, mrnId };
      const requestClone = Object.assign({}, request, { body: { ...insuranceUpdate, cohortId } });
      const messageId: string = await publishMemberToPubsub(requestClone, idsForAttribution);
      console.log(`Updated Commons with latest insurances [memberId: ${memberId}, insuranceUpdate: ${JSON.stringify(insuranceUpdate)}]`);
      return response.status(200).json({ messageId });
    } catch (e) {
      console.error(`Error publishing attribution message to Pub/Sub on member resync [memberId: ${memberId}, error: ${e}]`);
      return next(e);
    }
  } catch (e) {
    console.error(`Error resyncing member [error: ${e}]`);
    return next(e);
  }
}
