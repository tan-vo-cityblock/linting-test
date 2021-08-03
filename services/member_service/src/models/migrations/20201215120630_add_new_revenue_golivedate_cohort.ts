import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.table('cohort', (t) => {
    t.date('revenueGoLiveDate');
  });

  //   This cohort is never used and is a dead entry
  await knex('cohort').where({ id: 8 }).del();

  // Emblem
  await knex('cohort').where({ id: 1 }).update({ revenueGoLiveDate: '2018-07-01' });
  await knex('cohort').where({ id: 2 }).update({ revenueGoLiveDate: '2018-09-01' });
  await knex('cohort').where({ id: 3 }).update({ revenueGoLiveDate: '2018-12-01' });
  await knex('cohort').where({ id: 4 }).update({ revenueGoLiveDate: '2019-02-01' });
  await knex('cohort').where({ id: 7 }).update({ revenueGoLiveDate: '2019-05-01' });
  await knex('cohort').where({ id: 11 }).update({ revenueGoLiveDate: '2019-08-01' });
  await knex('cohort').where({ id: 12 }).update({ revenueGoLiveDate: '2019-08-28' });
  await knex('cohort').where({ id: 13 }).update({ revenueGoLiveDate: '2019-11-04' });
  await knex('cohort').where({ id: 18 }).update({ revenueGoLiveDate: '2020-05-01' });
  await knex('cohort').where({ id: 20 }).update({ revenueGoLiveDate: '2020-07-01' });
  await knex('cohort').where({ id: 21 }).update({ revenueGoLiveDate: '2020-09-01' });
  await knex('cohort').where({ id: 24 }).update({ revenueGoLiveDate: '2021-01-01' });

  // Connecticare
  await knex('cohort').where({ id: 5 }).update({ revenueGoLiveDate: '2019-03-01' });
  await knex('cohort').where({ id: 9 }).update({ revenueGoLiveDate: '2019-06-01' });
  await knex('cohort').where({ id: 10 }).update({ revenueGoLiveDate: '2019-07-01' });
  await knex('cohort').where({ id: 14 }).update({ revenueGoLiveDate: '2020-01-01' });
  await knex('cohort').where({ id: 17 }).update({ revenueGoLiveDate: '2020-06-01' });

  // Tufts
  await knex('cohort').where({ id: 15 }).update({ revenueGoLiveDate: '2020-03-01' });
  await knex('cohort').where({ id: 16 }).update({ revenueGoLiveDate: '2020-04-01' });
  await knex('cohort').where({ id: 22 }).update({ revenueGoLiveDate: '2020-10-01' });
  await knex('cohort').where({ id: 19 }).update({ revenueGoLiveDate: '2020-04-29' });

  // CareFirst
  return knex('cohort').where({ id: 23 }).update({ revenueGoLiveDate: '2020-10-01' });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.table('cohort', (t) => {
    t.dropColumn('revenueGoLiveDate');
  });
}
