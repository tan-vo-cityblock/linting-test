import * as Knex from 'knex';
import { flatten } from 'lodash';

interface IMarketMeasure {
  marketId: number;
  measureId: number;
}

export async function formatMarketsMeasures(
  knex: Knex,
  marketsQms: Array<{ name: string; measures: string[] }>,
): Promise<IMarketMeasure[]> {
  const formattedMarketMeasuresPromises = marketsQms.map(async (marketQms) => {
    const marketResult = await knex
      .select('id')
      .from('market')
      .first()
      .where('name', marketQms.name);
    const measureResults = await knex
      .select('id')
      .from('measure')
      .whereIn('code', marketQms.measures)
      .andWhere('rateId', 0);

    return measureResults.map((measureResult) => ({
      marketId: marketResult.id,
      measureId: measureResult.id,
    }));
  });

  const formattedMarketMeasures = await Promise.all(formattedMarketMeasuresPromises);
  return Promise.all(flatten(formattedMarketMeasures));
}
