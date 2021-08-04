import { NextFunction, Response } from 'express';
import { Model } from 'objection';
import { publishMemberToPubsub, IdsForAttributionUpdate } from '../middleware/publish-member-to-pubsub';
import { memberValidation } from '../middleware/validate-member';
import { Address, IAddress } from '../models/address';
import { IMedicalRecordNumber, MedicalRecordNumber } from '../models/mrn';
import { getOrCreateTransaction, IRequestWithPubSub } from './utils';

export async function resyncMemberAddresses(
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
    // get latest addresses
    const { mrnId, addresses } = await getOrCreateTransaction(
      request.txn,
      Model.knex(),
      async (txn) => {
        const mrn: IMedicalRecordNumber = await MedicalRecordNumber.getMrn(memberId, txn);
        const activeAddresses: IAddress[] = await Address.getAllByMemberId(memberId, txn);
        return { mrnId: mrn && mrn.id, addresses: activeAddresses };
      }
    );

    const demographicsUpdates = { demographics: { shouldUpdateAddress: true, addresses } };

    // send to commons
    try {
      const idsForAttribution: IdsForAttributionUpdate = { patientId: memberId, mrnId };
      const requestClone = Object.assign({}, request, { body: { ...demographicsUpdates } });
      const messageId: string = await publishMemberToPubsub(requestClone, idsForAttribution);
      console.log(`Updated Commons with latest addresses [memberId: ${memberId}, demographicsUpdates: ${JSON.stringify(demographicsUpdates)}]`);
      return response.status(200).json({ messageId });
    } catch (e) {
      console.error(`Error publishing attribution message to Pub/Sub on member addresses resync [memberId: ${memberId}, error: ${e}]`);
      return next(e);
    }
  } catch (e) {
    console.error(`Error resyncing member addresses [error: ${e}]`);
    return next(e);
  }
}
