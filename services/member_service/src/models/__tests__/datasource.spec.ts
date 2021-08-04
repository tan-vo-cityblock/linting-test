import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { Datasource } from '../datasource';
import { CCI_DATASOURCE_ID } from '../seeds/static-ids';

describe('Datasource model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => {
    await txn.rollback();
  });

  it('fetches by name', async () => {
    const expectedResult = { id: CCI_DATASOURCE_ID, name: 'connecticare' };
    const result = await Datasource.getByName('connecticare', txn);
    expect(result).toMatchObject(expectedResult);
  });
});
