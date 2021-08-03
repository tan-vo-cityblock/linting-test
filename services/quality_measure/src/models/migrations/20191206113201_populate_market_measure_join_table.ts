import * as Knex from 'knex';
import { formatMarketsMeasures } from './helpers/market_helpers';

const nyCtMeasures = [
  'HEDIS ABA',
  'ABLE AWV1',
  'ABLE AWV2',
  'HEDIS FVA',
  'HEDIS FVO',
  'HEDIS BCS',
  'HEDIS CCS',
  'HEDIS COL',
  'HEDIS CDC1',
  'HEDIS CDC5',
  'HEDIS CDC6',
];
const ncMeasures = ['HEDIS ABA', 'HEDIS BCS', 'HEDIS CCS', 'HEDIS CDC6'];
const marketsAndMeasures = [
  {
    name: 'New York City',
    measures: nyCtMeasures,
  },
  {
    name: 'Connecticut',
    measures: nyCtMeasures,
  },
  {
    name: 'Charlotte',
    measures: ncMeasures,
  },

  {
    name: 'Fayetteville',
    measures: ncMeasures,
  },
  {
    name: 'Piedmont Triad',
    measures: ncMeasures,
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
