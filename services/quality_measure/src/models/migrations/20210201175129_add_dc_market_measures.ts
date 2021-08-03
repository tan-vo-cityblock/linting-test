import * as Knex from 'knex';
import { formatMarketsMeasures } from './helpers/market_helpers';

const dcMeasures = ['HEDIS FVA', 'HEDIS FVO', 'HEDIS BCS', 'HEDIS CDC3'];

const marketsAndMeasures = [
  {
    name: 'CareFirst DC',
    measures: dcMeasures,
  },
];

export async function up(knex: Knex): Promise<any> {
  const marketsMeasures = await formatMarketsMeasures(knex, marketsAndMeasures);
  return knex('market_measure').insert(marketsMeasures);
}

export async function down(knex: Knex): Promise<any> {
  const marketsMeasures = await formatMarketsMeasures(knex, marketsAndMeasures);
  const marketsMeasuresPairs = marketsMeasures.map((marketMeasure) => [
    marketMeasure.marketId,
    marketMeasure.measureId,
  ]);
  return knex('market_measure').whereIn(['marketId', 'measureId'], marketsMeasuresPairs).del();
}
