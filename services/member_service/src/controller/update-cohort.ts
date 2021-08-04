import { Request, Response } from 'express';
import { Model, Transaction } from 'objection';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import { Cohort } from '../models/cohort';

// tslint:disable no-console
export async function updateCohort(request: Request, response: Response) {
  const { memberId } = request.params;
  const { cohortName } = request.body;

  try {
    const member = await transaction(Model.knex(), async (txn) => {
      const errors: string[] = await requestValidate(memberId, cohortName, txn);

      if (errors.length > 0) {
        throw new Error(errors.toString());
      }

      return Member.updateCohort(memberId, cohortName, txn);
    });

    console.log(`Updated cohort for member [memberId: ${memberId}, cohortName: ${cohortName}]`);
    return response.send(member);
  } catch (e) {
    const failedRequirement = `Unable to update cohort for member: ${memberId}`;
    console.log(failedRequirement);
    return response.status(422).json({ error: failedRequirement, body: e });
  }
}

async function requestValidate(memberId: string, cohortName: string, txn: Transaction) {
  const errorBody = [];
  let failedRequirement: string;

  if (!cohortName) {
    failedRequirement = 'field "cohortName" missing in the body of the request';
    console.error(`Unable to update cohort for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  
  if (!Cohort.getByName(cohortName, txn)) {
    failedRequirement = `Attempt to update to a cohort that does not exist [cohortName: ${cohortName}]`;
    console.error(`Unable to update cohort for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  
  if (!validate(memberId)) {
    failedRequirement = `The memberId: ${memberId} is not a valid UUID`;
    console.error(`Unable to update cohort for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);
  if (!member) {
    failedRequirement = `Member does not exist [memberId: ${memberId}]`;
    console.error(`Unable to update cohort for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  return errorBody;
}
// tslint:enable no-console
