import { Request, Response } from 'express';
import { transaction } from 'objection';
import { ElationMap } from '../models/elation-map';
import { Measure } from '../models/measure';

// TODO: WRITE TESTS FOR THIS FILE

// tslint:disable no-console
export async function getMeasures(_: Request, response: Response) {
  try {
    const measuresResponse = await transaction(Measure.knex(), async (txn) => {
      return Measure.getAll(txn);
    });
    return response.status(200).json(measuresResponse);
  } catch (e) {
    console.error('error in the controller get-measures.getMeasuresCodeRateIds', e);
    return response.status(500).json({ error: e.message });
  }
}

export async function getElationMappedMeasures(_: Request, response: Response) {
  try {
    const elationMapResponse = await transaction(ElationMap.knex(), async (txn) => {
      const elationMappedMeasures = await ElationMap.getAllEager(txn);
      return elationMappedMeasures.map((elationMappedMeasure) => ({
        codeType: elationMappedMeasure.codeType,
        code: elationMappedMeasure.code,
        measureId: elationMappedMeasure.measure.id,
        statusName: elationMappedMeasure.memberMeasureStatus.name,
      }));
    });

    return response.status(200).json(elationMapResponse);
  } catch (e) {
    console.error('error in the controller get-measures.getMeasuresCodeRateIds', e);
    return response.status(500).json({ error: e.message });
  }
}

// tslint:enable no-console
