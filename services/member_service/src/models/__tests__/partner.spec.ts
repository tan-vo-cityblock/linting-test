import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { Partner } from '../partner';
import { EMBLEM_PARTNER_ID } from '../seeds/static-ids';

describe('Partner model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('fetches by name', async () => {
    const result = await Partner.getByName('emblem', txn);
    expect(result.name).toBe('emblem');
    expect(result.id).toBe(EMBLEM_PARTNER_ID);
  });
});
