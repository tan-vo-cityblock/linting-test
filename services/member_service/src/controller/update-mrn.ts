import { Request, Response } from 'express';
import { Model, Transaction } from 'objection';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import { MedicalRecordNumber } from '../models/mrn';

// tslint:disable no-console
export async function updateMrn(request: Request, response: Response) {
  const { memberId } = request.params;
  const { id, name } = request.body;

  try {
    const mrn = await transaction(Model.knex(), async (txn) => {
      const errors: string[] = await requestValidate(memberId, id, name, txn);

      if (errors.length > 0) {
        throw new Error(errors.toString());
      }

      return MedicalRecordNumber.updateOrCreateMrn(memberId, id, name, txn);
    });

    console.log(`Updated MRN for member [memberId: ${memberId}, mrn: ${mrn}]`);
    return response.send(mrn);
  } catch (e) {
    const failedRequirement = `Unable to update MRN for member: ${memberId}`;
    console.log(failedRequirement);
    return response.status(422).json({ error: failedRequirement, body: e });
  }
}

async function requestValidate(memberId: string, mrnId: string, mrnName: string, txn: Transaction) {
  const errorBody = [];
  let failedRequirement: string;
  if (!mrnId) {
    failedRequirement = 'field "id" missing in the body of the request';
    console.error(`Unable to update MRN for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  if (!mrnName) {
    failedRequirement = 'field "name" missing in the body of the request';
    console.error(`Unable to update MRN for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  if (!validate(memberId)) {
    failedRequirement = `The memberId: ${memberId} is not a valid UUID`;
    console.error(`Unable to update MRN for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);
  if (!member) {
    failedRequirement = `Member does not exist [memberId: ${memberId}]`;
    console.error(`Unable to update MRN for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  return errorBody;
}
// tslint:enable no-console
