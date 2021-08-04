import * as Knex from 'knex';
import { formatMarketsMeasures } from './helpers/market_helpers';

const fluMeasures = ['ABLE FVA', 'ABLE FVO'];

const markets = [
  {
    name: 'Massachusetts',
  },
  {
    name: 'CareFirst DC',
  },
  {
    name: 'Connecticut',
  },
  {
    name: 'New York City',
  },
];

const marketsAndMeasures = markets.map((market) => {
  return {
    name: market.name,
    measures: fluMeasures,
  };
});

export async function up(knex: Knex): Promise<void> {
  const marketsMeasures = await formatMarketsMeasures(knex, marketsAndMeasures);
  return knex('market_measure').insert(marketsMeasures);
}

export async function down(knex: Knex): Promise<void> {
  const marketsMeasures = await formatMarketsMeasures(knex, marketsAndMeasures);
  const marketsMeasuresPairs = marketsMeasures.map((marketMeasure) => [
    marketMeasure.marketId,
    marketMeasure.measureId,
  ]);
  return knex('market_measure').whereIn(['marketId', 'measureId'], marketsMeasuresPairs).del();
}
