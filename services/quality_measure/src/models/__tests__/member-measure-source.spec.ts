import { transaction } from 'objection';
import { setupDb } from '../../util/test-utils';
import { MemberMeasureSource } from '../member-measure-source';

describe('MemberMeasureSource model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(MemberMeasureSource.knex());
  });

  afterEach(async () => {
    await txn.rollback();
  });

  it('fetches sources correctly by name', async () => {
    const able = await MemberMeasureSource.getByName('able', txn);
    const commons = await MemberMeasureSource.getByName('commons', txn);
    const elation = await MemberMeasureSource.getByName('elation', txn);
    const qmService = await MemberMeasureSource.getByName('qm_service', txn);
    expect(able.name).toBe('able');
    expect(commons.name).toBe('commons');
    expect(elation.name).toBe('elation');
    expect(qmService.name).toBe('qm_service');
  });
});
