import { NextFunction, Response } from 'express';
import { createMemberElation } from '../middleware/create-member-elation';
import { createMemberInternal } from '../middleware/create-member-internal';
import { demographicsValidation } from '../middleware/validate-request';
import { deletePatientInElation } from '../util/elation';
import { IRequestWithTransaction } from './utils';

export async function createMember(
  request: IRequestWithTransaction,
  response: Response,
  next: NextFunction,
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
  } catch (e) {
    console.error('unable to create the member in Elation, see Elation errors earlier in stack');
    return next(e);
  }

  // Create the member and rollback elation creation if fails.
  try {
    const responseBody = await createMemberInternal(request, elationId);
    return response.send(responseBody);
  } catch (e) {
    console.error('unable to create the member, rolling back elation creation', e);
    if (!!elationId) await deletePatientInElation(elationId);
    return next(e);
  }
}
