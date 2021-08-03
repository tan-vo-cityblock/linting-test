import * as Knex from 'knex';
import slugify from 'slugify';

const newMarketName = 'Virtual';

export async function up(knex: Knex): Promise<any> {
  const marketsWithSlugs = {
    name: newMarketName,
    slug: slugify(newMarketName, { lower: true }),
  };
  return knex('market').insert(marketsWithSlugs);
}

export async function down(knex: Knex): Promise<any> {
  return knex('market').where('name', newMarketName).del();
}
