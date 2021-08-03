import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { Member } from '../member';
import { ELATION, MedicalRecordNumber } from '../mrn';
import { expectedMrnFilterMapping } from './test-data';

describe('MedicalRecordNumber model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('successfully updates an mrnId for a member', async () => {
    const nonUpdatedMember = await Member.get('a3cdcb8e-d960-11e9-b350-acde48001122', txn);
    const expected = {
      id: 'Dummy',
      name: ELATION,
    };
    const result = await MedicalRecordNumber.updateOrCreateMrn(
      'a74bcb4a-d964-11e9-8372-acde48001122',
      'Dummy',
      ELATION,
      txn,
    );
    expect(nonUpdatedMember.mrn.id).toBeDefined();
    expect(result).toEqual(expected);
  });

  it('successfully creates an mrn entry for a member', async () => {
    const expected = {
      id: 'Dummy',
      name: ELATION,
    };
    const result = await MedicalRecordNumber.updateOrCreateMrn(
      'a3cdcb8e-d960-11e9-b350-acde48001122',
      'Dummy',
      ELATION,
      txn,
    );
    const member = await Member.get('a3cdcb8e-d960-11e9-b350-acde48001122', txn);
    expect(result).toEqual(expected);
    expect(member.mrn.id).toEqual(expected.id);
    expect(member.mrn.name).toEqual(expected.name);
  });

  it('sucessfully soft deletes an mrn ID for a member', async () => {
    const memberId = '0d2e4cc6-e153-11e9-988e-acde48001122';
    const expected = {
      mrn: 'E5',
      name: ELATION,
      deletedReason: 'testing deletion on model',
    };
    const deletedMrn = await MedicalRecordNumber.deleteMrnIfExists(
      memberId,
      expected.deletedReason,
      txn,
    );

    expect(deletedMrn).toBeDefined();
    expect(deletedMrn.deletedAt).toBeDefined();
    expect(deletedMrn).toMatchObject(expected);
  });

  it('successfully gets member mrn mapping', async () => {
    const memberMapping = await MedicalRecordNumber.getMrnMapping({}, txn);
    expect(memberMapping.length).toEqual(4);
    expect(memberMapping).toMatchObject(expect.arrayContaining(expectedMrnFilterMapping));
  });

  it('successfully gets member mrn mapping based on their externalId and carrier', async () => {
    const memberMapping = await MedicalRecordNumber.getMrnMapping(
      { carrier: 'elation', externalId: 'E1' },
      txn,
    );

    const expected = [
      {
        carrier: 'elation',
        current: true,
        externalId: 'E1',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
      },
    ];

    expect(memberMapping).toMatchObject(expected);
    expect(memberMapping.length).toBe(1);
  });
});
