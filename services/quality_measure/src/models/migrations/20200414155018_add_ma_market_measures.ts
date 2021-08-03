import * as Knex from 'knex';
import { formatMarketsMeasures } from './helpers/market_helpers';

const maMeasures = [
  'HEDIS ABA',
  'HEDIS FVA',
  'HEDIS FVO',
  'HEDIS BCS',
  'HEDIS COL',
  'HEDIS CDC5',
  'HEDIS AAP1',
  'HEDIS CBP',
  'HEDIS CDC3',
  'HEDIS CHL',
];

const marketsAndMeasures = [
  {
    name: 'Massachusetts',
    measures: maMeasures,
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
