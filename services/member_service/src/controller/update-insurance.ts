import { Response } from 'express';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';
import { compact, difference, flatten, isEmpty } from 'lodash';
import { Model, Transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import {
  IConsolidateAndUpdateDetailsInput,
  IInsurance,
  IUpdatedInsuranceStatus,
  MemberInsurance
} from '../models/member-insurance';
import { IUpdateInsuranceRequest } from './types';

export interface IUpdateInsuranceResponse {
  updatedInsuranceStatuses: IUpdatedInsuranceStatus[]
}

// tslint:disable no-console
export async function updateInsurance(request: IRequestWithTransaction, response: Response) {
  const { carrier, plans }: IUpdateInsuranceRequest = request.body;
  const { memberId } = request.params;
  const consolidateAndUpdateDetailsInput: IConsolidateAndUpdateDetailsInput = { memberId, carrier, plans };

  try {
    const updatedInsuranceStatuses: IUpdatedInsuranceStatus[] = await getOrCreateTransaction(request.txn, Model.knex(), async (txn) => {
      const errors = await requestValidation(consolidateAndUpdateDetailsInput, txn);
      if (errors.length > 0) {
        throw new Error(errors.toString());
      }

      return MemberInsurance.consolidateInsurancesAndUpdateDetails(consolidateAndUpdateDetailsInput, txn);
    });

    console.log(
      `Updated member insurance for member [memberId: ${memberId}, updatedInsuranceStatuses: ${JSON.stringify(updatedInsuranceStatuses)}]`,
    );
    return response.send({ updatedInsuranceStatuses });
  } catch (e) {
    const failedRequirement = `Unable to update insurance for [memberId: ${memberId}]`;
    console.log('Unable to update external Id for member', e);
    return response.status(422).json({ error: failedRequirement, body: e });
  }
}
// tslint:enable no-console

async function requestValidation(
  { memberId, carrier, plans }: IConsolidateAndUpdateDetailsInput,
  txn: Transaction,
) {
  const errorBody = [];
  if (!carrier) {
    const failedRequirement = 'field "carrier" missing in the body of the request';
    errorBody.push(failedRequirement);
  }
  if (isEmpty(plans)) {
    const failedRequirement = 'field "plans" missing in the body of the request';
    errorBody.push(failedRequirement);
  }
  if (!validate(memberId)) {
    const failedRequirement = `The memberId: ${memberId} is not a valid UUID`;
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const member = await Member.get(memberId, txn);

  if (!member) {
    const failedRequirement = `member ${memberId} does not exist`;
    errorBody.push(failedRequirement);
    return errorBody;
  }

  const insuranceIdsToUpdate: string[] = plans.map((plan) => plan.externalId);

  if (compact(insuranceIdsToUpdate).length !== plans.length) {
    const failedRequirement = `member ${memberId} must provide an id for every insurance plan requested to update`;
    errorBody.push(failedRequirement);
  }

  const existingInsuranceIds: string[] = flatten(
    member.insurances.map((insurance: IInsurance) => insurance.plans.map((plan) => plan.externalId)),
  );
  const invalidInsuranceIds = difference(insuranceIdsToUpdate, existingInsuranceIds);

  if (!isEmpty(invalidInsuranceIds)) {
    const failedRequirement = `member ${memberId} does not have provided insurances: ${invalidInsuranceIds}`;
    errorBody.push(failedRequirement);
  }
  return errorBody;
}
