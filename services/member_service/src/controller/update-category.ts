import { Request, Response } from 'express';
import { Model, Transaction } from 'objection';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import { categoryNames } from '../models/category';

// tslint:disable no-console
export async function updateCategory(request: Request, response: Response) {
  const { memberId } = request.params;
  const { category } = request.body;

  try {
    const member = await transaction(Model.knex(), async (txn) => {
      const errors: string[] = await requestValidate(memberId, category, txn);

      if (errors.length > 0) {
        throw new Error(errors.toString());
      }

      return Member.updateCategory(memberId, category, txn);
    });

    console.log(`Updated category for member [memberId: ${memberId}, category: ${category}]`);
    return response.send(member);
  } catch (e) {
    const failedRequirement = `Unable to update category for member: ${memberId}`;
    console.log(failedRequirement);
    return response.status(422).json({ error: failedRequirement, body: e });
  }
}

async function requestValidate(memberId: string, category: string, txn: Transaction) {
  const errorBody = [];
  let failedRequirement: string;

  if (!category) {
    failedRequirement = 'field "category" missing in the body of the request';
    console.error(`Unable to update category for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  
  if (!categoryNames.includes(category)) {
    failedRequirement = `[category: ${category}] does not belong to the set of [categoryNames: ${categoryNames}]`;
    console.error(`Unable to update category for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  
  if (!validate(memberId)) {
    failedRequirement = `The memberId: ${memberId} is not a valid UUID`;
    console.error(`Unable to update category for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);
  if (!member) {
    failedRequirement = `Member does not exist [memberId: ${memberId}]`;
    console.error(`Unable to update category for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  return errorBody;
}
// tslint:enable no-console
