import { NextFunction, Response } from 'express';
import { createMemberElation } from '../middleware/create-member-elation';
import { createMemberInternal } from '../middleware/create-member-internal';
import { IdsForAttributionCreate, publishMemberToPubsub } from '../middleware/publish-member-to-pubsub';
import { demographicsValidation } from '../middleware/validate-request';
import { deletePatientInElation } from '../util/elation';
import { IRequestWithPubSub } from './utils';

export async function createAndPublishMember(
  request: IRequestWithPubSub,
  response: Response,
  next: NextFunction
) {
  let elationId: string;

  // First perform some basic request validation
  const errors = await demographicsValidation(request);
  if (errors.length > 0) {
    return response.status(422).json({ error: errors });
  }

  // Create the member in elation
  try {
    elationId = await createMemberElation(request);
    // tslint:disable-next-line:no-console
    console.log(`Created elationId for member on publish route [elationId: ${elationId}]`);
  } catch (e) {
    console.error('unable to create the member in elation', e);
    return response.status(500).json({ error: e.message });
  }

  // Create the member and publish and rollback elation creation if fails.
  try {
    const memberResponse = await createMemberInternal(request, elationId);
    const { patientId, cityblockId } = memberResponse;
    // tslint:disable-next-line:no-console
    console.log(`Created member in member service on publish route [memberId: ${patientId}]`);

    const idsForAttribution: IdsForAttributionCreate = { patientId, cityblockId, mrnId: elationId };
    const messageId: string = await publishMemberToPubsub(request, idsForAttribution);

    const responseBody = {
      patientId,
      cityblockId,
      mrn: elationId || null,
      messageId,
    };

    return response.send(responseBody);
  } catch (e) {
    console.error('unable to create the member, rolling back elation creation', e);
    if (!!elationId) {
      await deletePatientInElation(elationId);
    }
    return next(e);
  }
}
