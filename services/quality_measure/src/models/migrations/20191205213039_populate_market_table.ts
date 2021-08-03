import * as Knex from 'knex';
import slugify from 'slugify';

const markets = [
  {
    name: 'New York City',
  },
  {
    name: 'Connecticut',
  },
  {
    name: 'Charlotte',
  },
  {
    name: 'Fayetteville',
  },
  {
    name: 'Piedmont Triad',
  },
];

export async function up(knex: Knex): Promise<any> {
  const marketsWithSlugs = markets.map((market) => ({
    ...market,
    slug: slugify(market.name, { lower: true }),
  }));
  return knex('market').insert(marketsWithSlugs);
}

export async function down(knex: Knex): Promise<any> {
  const marketNames = markets.map((market) => market.name);
  return knex('market').whereIn('name', marketNames).del();
}
