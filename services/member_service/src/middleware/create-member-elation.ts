import { Request } from 'express';
import { ICreateAndPublishMemberRequest } from '../controller/types';
import { createPatientInElation, isElationCallNecessary } from '../util/elation';

export async function createMemberElation(request: Request) {
  const body: ICreateAndPublishMemberRequest = request.body;
  if (isElationCallNecessary(body)) {
    let elationResponse;
    try {
      elationResponse = await createPatientInElation(body);
      return elationResponse && String(elationResponse.id);
    } catch (e) {
      const errorTag = `demographics: ${JSON.stringify(body.demographics)}`;
      throw new Error(`failed to create member in elation [${errorTag}]`);
    }
  } else {
    return null;
  }
}
