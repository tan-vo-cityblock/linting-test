import { Request, Response } from 'express';
import { compact } from 'lodash';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import validator from 'validator';
import {
  IMemberMeasureRequest,
  IMemberMeasureResponse,
  MemberMeasure,
} from '../models/member-measure';
import { SourceNames } from '../models/member-measure-source';
import { isTruthyNumber, isTruthyString, isValidUuid } from '../util/helper';

function checkMeasureCodeAndRateId(measureStatusReq: IMemberMeasureRequest): boolean {
  const { code, rateId } = measureStatusReq;
  const rateIdIsDefined = rateId !== undefined;
  const rateIdExistsAsNumberOrNull =
    isTruthyNumber(rateId) || (rateIdIsDefined && (rateId === null || rateId === 0));
  return isTruthyString(code) && rateIdExistsAsNumberOrNull;
}

function checkMeasureIdentifiers(measureStatusReq: IMemberMeasureRequest): boolean {
  const { id } = measureStatusReq;
  return isTruthyNumber(id) || checkMeasureCodeAndRateId(measureStatusReq);
}

function checkMeasureStatusTypes(measureStatusReq: IMemberMeasureRequest) {
  const { status, setOn } = measureStatusReq;
  const setOnExistsAsDateString = isTruthyString(setOn) && validator.isISO8601(setOn);

  return isTruthyString(status) && setOnExistsAsDateString;
}

function checkUserId(measureStatusReq: IMemberMeasureRequest): boolean {
  const { userId } = measureStatusReq;

  return isValidUuid(userId);
}

function checkReasons(measureStatusReq: IMemberMeasureRequest): boolean {
  const { reason } = measureStatusReq;

  const reasonIsDefined = reason !== undefined;
  return isTruthyString(reason) || (reasonIsDefined && reason === null) || !reasonIsDefined;
}

function checkPerformanceYear(measureStatusReq: IMemberMeasureRequest): boolean {
  const { performanceYear } = measureStatusReq;
  return isTruthyNumber(performanceYear);
}

function checkPerformanceYearDefined(measureStatusReq: IMemberMeasureRequest): boolean {
  const { performanceYear } = measureStatusReq;
  return !!performanceYear;
}

function validateRequestBody(request: Request, sourceName: string) {
  const errorBody = [];

  if (request.body instanceof Array === false) {
    errorBody.push('Request body must be a json array.');
    return errorBody;
  }

  if (request.body.every(checkMeasureIdentifiers) === false) {
    errorBody.push(
      'array objects must have field {id: string}, or fields {code: string, rateId: integer | null}',
    );
  }

  if (request.body.every(checkMeasureStatusTypes) === false) {
    errorBody.push('array objects must have field {status: string}');
    errorBody.push('array objects must have field {setOn: string} with ISO8601 format');
  }

  if (request.body.every(checkReasons) === false) {
    errorBody.push('array objects reason field must be a string if it is defined');
  }

  if (sourceName === SourceNames.commons && request.body.every(checkUserId) === false) {
    errorBody.push('array objects must provide userId field as a valid uuid');
  }

  if (sourceName === SourceNames.able && request.body.every(checkPerformanceYear) === false) {
    errorBody.push('array objects from Able must provide a numeric performanceYear');
  }

  if (sourceName !== SourceNames.able && request.body.every(checkPerformanceYearDefined) === true) {
    errorBody.push('array objects not from Able cannot provide performanceYear property');
  }

  return errorBody;
}

// tslint:disable no-console
export async function addMemberMeasures(request: Request, response: Response) {
  if (!validate(request.params.memberId)) {
    console.error('Invalid uuid provided as memberId parameter');
    return response.status(400).json({ error: 'Invalid uuid provided as memberId parameter' });
  }
  // TODO: Post memberId to Member Service to confirm member enroll status and avoid storing bad data.
  const { memberId } = request.params;
  const { sourceName } = response.locals;

  const errors = validateRequestBody(request, sourceName);

  if (errors.length > 0) {
    console.error('Invalid Json Request', errors);
    return response.status(400).json({ errors });
  }

  try {
    const memberMeasuresResponse: IMemberMeasureResponse[] = await transaction(
      MemberMeasure.knex(),
      async (txn) => {
        const memberMeasuresAdded = await MemberMeasure.insertOrPatchMemberMeasures(
          memberId,
          sourceName,
          request.body,
          txn,
        );

        return compact(memberMeasuresAdded).map((memberMeasure) => ({
          id: memberMeasure.measureId,
          displayName: memberMeasure.measure.displayName,
          status: memberMeasure.memberMeasureStatus.name,
          type: memberMeasure.measure.type,
        }));
      },
    );

    if (sourceName === SourceNames.able || sourceName === SourceNames.elation) {
      return response.status(200).send('OK');
    } else {
      return response.status(200).json(memberMeasuresResponse);
    }
  } catch (e) {
    console.error('error in the controller add-member-measures', e);
    return response.status(500).json({ error: e.message });
  }
}
// tslint:enable no-console
