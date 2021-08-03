import { transaction } from 'objection';
import { setupDb } from '../../util/test-utils';
import { MemberMeasureStatus } from '../member-measure-status';

const expectedResult = [
  'provOpen',
  'open',
  'provClosed',
  'closed',
  'failed',
  'excluded',
  'deniedService',
];

describe('MemberMeasureStatus model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(MemberMeasureStatus.knex());
  });

  afterEach(async () => {
    await txn.rollback();
  });

  it('fetches all statuses', async () => {
    const result = await MemberMeasureStatus.getAll(txn);
    const statusNames = result.map((status) => status.name);
    expect(result).toHaveLength(expectedResult.length);
    expect(statusNames).toEqual(expectedResult);
  });
});
