import { setupDb, cleanDb, mockTxn, destroyDb } from '../../lib/test-utils';
import { MemberEligibilities } from '../member-eligibilities';
import { Member } from '../member';
import { EMBLEM_COHORT1_ID, HIGH_COST_CATEGORY_ID } from '../seeds/static-ids';

describe('MemberEligibilities', () => {
  beforeAll(async () => setupDb());

  beforeEach(async () => cleanDb());

  afterAll(async () => destroyDb());

  describe('upsertForMember', () => {
    it('upserts harp status', async () => {
      const member = await Member.create(
        'emblem',
        'commons',
        EMBLEM_COHORT1_ID,
        HIGH_COST_CATEGORY_ID,
        null,
        mockTxn,
      );
      const inserted = await MemberEligibilities.upsertForMember(member.id, {
        commonsHarpStatus: 'connected',
      });
      expect(inserted).toMatchObject({ memberId: member.id, commonsHarpStatus: 'connected' });
      const upserted = await MemberEligibilities.upsertForMember(member.id, {
        commonsHarpStatus: 'approved',
      });
      expect(upserted).toMatchObject({ memberId: member.id, commonsHarpStatus: 'approved' });
      expect((await MemberEligibilities.query().count())[0].count).toEqual('1')
    });
  });
});
