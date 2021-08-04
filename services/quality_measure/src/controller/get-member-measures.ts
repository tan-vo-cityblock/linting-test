import { Request, Response } from 'express';
import { compact } from 'lodash';
import { transaction } from 'objection';
import validate from 'uuid-validate';
import { IMemberMeasureResponse, MemberMeasure } from '../models/member-measure';
import { isTruthyString } from '../util/helper';

// TODO: WRITE TESTS FOR THIS FILE

// tslint:disable no-console
export async function getMemberMeasures(request: Request, response: Response) {
  const { memberId } = request.params;
  const { marketSlug } = request.query;

  if (!validate(memberId)) {
    console.error('Invalid uuid provided as memberId parameter');
    return response.status(400).json({ error: 'Invalid uuid provided as memberId parameter' });
  }

  if (!isTruthyString(marketSlug)) {
    console.error('Invalid value provided as marketSlug');
    return response.status(400).json({ error: 'marketSlug query parameter must be a string' });
  }

  try {
    const memberMeasuresResponse: IMemberMeasureResponse[] = await transaction(
      MemberMeasure.knex(),
      async (txn) => {
        const memberMeasures = await MemberMeasure.getMemberMeasures(memberId, marketSlug, txn);
        return compact(memberMeasures).map((memberMeasure) => ({
          id: memberMeasure.measureId,
          displayName: memberMeasure.measure.displayName,
          status: memberMeasure.memberMeasureStatus.name,
          type: memberMeasure.measure.type,
        }));
      },
    );
    return response.status(200).json(memberMeasuresResponse);
  } catch (e) {
    console.error('error in the controller get-member-measures', e);
    return response.status(500).json({ error: e.message });
  }
}
// tslint:enable no-console
