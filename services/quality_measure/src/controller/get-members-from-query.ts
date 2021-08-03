import { Request, Response } from 'express';
import { compact, isArray, isEmpty, uniq } from 'lodash';
import { transaction } from 'objection';
import { MemberMeasure } from '../models/member-measure';
import { MemberMeasureStatus } from '../models/member-measure-status';

import { isTruthyNumericString, isTruthyString, isValidUuid } from '../util/helper';

// tslint:disable no-console

function validateRequestQueriesAndBody(
  queryMeasureIds: any,
  queryMeasureStatusNames: any,
  bodyMemberIds: any,
) {
  const errorBody = [];

  if (!!queryMeasureIds && !queryMeasureIds.every(isTruthyNumericString)) {
    errorBody.push(
      'Request query parameter [measureIds] must be a numeric string or an array of numeric strings.',
    );
  }

  if (!!queryMeasureStatusNames && !queryMeasureStatusNames.every(isTruthyString)) {
    errorBody.push(
      'Request query parameter [measureStatusNames] must be a string or an array of strings.',
    );
  }

  if (!queryMeasureIds.length && !queryMeasureStatusNames.length) {
    errorBody.push(
      'Request query parameter must include at least one of [measureIds, measureStatusNames].',
    );
  }

  if (!isArray(bodyMemberIds) || (isArray(bodyMemberIds) && isEmpty(bodyMemberIds))) {
    errorBody.push('Request body must be a non-empty array.');
  }
  if (!bodyMemberIds.every(isValidUuid)) {
    errorBody.push('Request body must be an array of valid uuids as strings.');
  }

  return errorBody;
}
export async function getMembersFromQuery(request: Request, response: Response) {
  const query = request.query;

  const queryMeasureIds = isArray(query.measureIds)
    ? query.measureIds
    : compact([query.measureIds]);

  const queryMeasureStatusNames = isArray(query.measureStatusNames)
    ? query.measureStatusNames
    : compact([query.measureStatusNames]);

  const { memberIds: bodyMemberIds } = request.body;

  const errors = validateRequestQueriesAndBody(
    queryMeasureIds,
    queryMeasureStatusNames,
    bodyMemberIds,
  );

  if (errors.length > 0) {
    console.error('Invalid Query Request', errors);
    return response.status(400).json({ errors });
  }

  try {
    const memberIds: string[] = uniq(bodyMemberIds);

    const memberIdsResponse = await transaction(MemberMeasure.knex(), async (txn) => {
      const measureIds: number[] = uniq(queryMeasureIds).map(Number);
      let measureStatusIds: number[];

      if (!isEmpty(queryMeasureStatusNames)) {
        measureStatusIds = await MemberMeasureStatus.getIdsByNames(
          uniq(queryMeasureStatusNames),
          txn,
        );
      } else {
        measureStatusIds = await MemberMeasureStatus.getAllIds(txn);
      }

      const memberMeasuresFromQuery = await MemberMeasure.getMemberMeasuresFromQuery(
        measureIds,
        measureStatusIds,
        memberIds,
        txn,
      );
      return uniq(compact(memberMeasuresFromQuery).map((memberMeasure) => memberMeasure.memberId));
    });

    return response.status(200).json(memberIdsResponse);
  } catch (e) {
    console.error('error in the controller get-members-from-query', e);
    return response.status(500).json({ error: e.message });
  }
}
