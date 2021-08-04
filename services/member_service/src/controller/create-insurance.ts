import { NextFunction, Response } from 'express';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import { Member } from '../models/member';
import { IInsurancePlan, IInsurance } from '../models/member-insurance';
import { MemberInsurance } from '../models/member-insurance';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';
import { updateCityblockMemberInElation } from '../util/elation';

// tslint:disable no-console
export async function createInsurance(request: IRequestWithTransaction, response: Response, next: NextFunction) {
  const { carrier, plans }: IInsurance = request.body;
  const updateElation = request.body.updateElation;
  const { memberId } = request.params;

  const errors = await requestValidation(memberId, carrier, plans);
  if (errors.length > 0) {
    return response.status(422).json({ error: errors });
  }

  try {
    if (!!updateElation) {
      const elationResponse = await updateCityblockMemberInElation(memberId, request.body);
      if (!elationResponse.success) {
        const elationStatusCode = elationResponse.statusCode;
        console.log(`Error updating insurance in elation for [memberId: ${memberId}, code: ${elationStatusCode}]`);
        return response.status(elationStatusCode).json(elationResponse);
      }
    }

    const createdInsurances = await getOrCreateTransaction(request.txn, MemberInsurance.knex(), async (txn) => {
      return MemberInsurance.createInsurance(memberId, carrier, plans, txn);
    });

    const createdInsuranceIds = createdInsurances.map((createdInsurance) => createdInsurance.id);
    console.log(`Created insurance ids [memberId: ${memberId}, insurances: ${createdInsuranceIds}]`);
    return response.send(createdInsurances);
  } catch (e) {
    console.log(`unable to create insurance for member: ${e}`);
    return next(e);
  }
}
// tslint:enable no-console

// tslint:disable no-console
async function requestValidation(memberId: string, carrier: string, plans: IInsurancePlan[]) {
  const errorBody = [];
  if (!carrier) {
    const failedRequirement = 'field "carrier" missing in the body of the request';
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  if (!plans) {
    const failedRequirement = 'field "plans" missing in the body of the request';
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  if (!validate(memberId)) {
    const failedRequirement = 'The memberId is not a valid UUID';
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
    return errorBody;
  }
  const member = await transaction(Member.knex(), async (txn) => {
    return Member.get(memberId, txn);
  });
  if (!member) {
    const failedRequirement = `Member does not exist [memberId: ${memberId}]`;
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errorBody.push(failedRequirement);
  }
  return errorBody;
}
// tslint:enable no-console
