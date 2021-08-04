import { transaction, Model } from 'objection';
import sleep from 'sleep-promise';
import { v4 as uuid } from 'uuid';
import { setupDb } from '../../lib/test-utils';
import { Address } from '../address';
import { Category } from '../category';
import { Email } from '../email';
import { IMemberUpdate, Member } from '../member';
import { IInsurance, MemberInsurance } from '../member-insurance';
import { MemberDemographics } from '../member-demographics';
import { Phone } from '../phone';
import { EMBLEM_COHORT1_ID, HIGH_COST_CATEGORY_ID } from '../seeds/static-ids';

jest.mock('../../util/elation', () => ({
  deletePatientIfElation: jest.fn(async (elationId, mrnName) => ({
    success: true,
    elationId,
  })),
}));

describe('member model', () => {
  let consolePlaceholder = null as any;
  let testDb = null as any;
  let txn = null as any;
  const userId: string = uuid();

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
    consolePlaceholder = jest.fn();
    /* tslint:disable no-console */
    console.log = consolePlaceholder;
    /* tslint:enable no-console */
  });

  afterEach(async () => txn.rollback());

  it('creates and retrieves a member', async () => {
    const createdMember = await Member.create(
      'emblem',
      'commons',
      EMBLEM_COHORT1_ID,
      HIGH_COST_CATEGORY_ID,
      null,
      txn,
    );
    const fetchedMember = await Member.get(createdMember.id, txn);

    expect(fetchedMember).not.toBeFalsy();
    expect(fetchedMember.id).toEqual(createdMember.id);
  });

  describe('get', () => {
    it('throws error on invalid id', async () => {
      await expect(Member.get('INVALID', txn)).rejects.toThrow();
    });

    it('returns null for non-existent id', async () => {
      const result = await Member.get('00000000-0000-1000-8000-000000000000', txn);
      expect(result).toBeNull();
    });

    it('fetches member by id', async () => {
      const result = await Member.get('a3cef19e-d960-11e9-b350-acde48001122', txn);
      expect(result.id).toBe('a3cef19e-d960-11e9-b350-acde48001122');
      await Member.delete('a3cef19e-d960-11e9-b350-acde48001122', null, null, txn);
    });
  });

  describe('getMultiple', () => {
    it('handles invalid partner', async () => {
      const results = await Member.getMultiple({ partner: 'INVALID' }, txn);
      expect(results).toHaveLength(0);
    });

    it('handles invalid datasource', async () => {
      const datasource = 'INVALID';
      try {
        await Member.getMultiple({ datasource }, txn);
      } catch (e) {
        expect(e).toEqual(new Error(`Invalid datasource name: ${datasource}`));
      }
    });

    it('filters by partner', async () => {
      const results: any = await Member.getMultiple({ partner: 'emblem' }, txn);
      expect(results).toHaveLength(17);
      results.forEach((member) => {
        expect(member.partner).toEqual('emblem');
      });
    });

    it('filters by datasource', async () => {
      const results: any = await Member.getMultiple({ datasource: 'elation' }, txn);
      expect(results).toHaveLength(4);
      results.forEach((member) => {
        const datasources = member.mrn.name;
        expect(datasources).toEqual('elation');
      });
    });

    it('filters by partner AND datasource', async () => {
      const results: any = await Member.getMultiple(
        { partner: 'connecticare', datasource: 'elation' },
        txn,
      );
      expect(results).toHaveLength(2);
      results.forEach((member) => {
        const datasources: string[] = member.insurances.map(
          (insurance: IInsurance) => insurance.carrier,
        );
        expect(member.mrn.name).toEqual('elation');
        expect(datasources).toEqual(['connecticare']);
        expect(member.partner).toEqual('connecticare');
      });
    });

    it('returns empty for empty memberIds filter', async () => {
      const results = await Member.getMultiple({ memberIds: [] }, txn);
      expect(results).toHaveLength(0);
    });

    it('throws error on invalid memberId', async () => {
      await expect(Member.getMultiple({ memberIds: ['INVALID'] }, txn)).rejects.toThrow();
    });

    it('returns empty for non-existent ids', async () => {
      const results = await Member.getMultiple(
        { memberIds: ['00000000-0000-1000-8000-000000000000'] },
        txn,
      );
      expect(results).toHaveLength(0);
    });

    it('filters by single memberId', async () => {
      const results: any = await Member.getMultiple(
        { memberIds: ['a3cef19e-d960-11e9-b350-acde48001122'] },
        txn,
      );
      expect(results).toHaveLength(1);
      expect(results[0].id).toEqual('a3cef19e-d960-11e9-b350-acde48001122');
    });

    it('filters by multiple memberIds', async () => {
      const results: any = await Member.getMultiple(
        {
          memberIds: [
            'a3cef19e-d960-11e9-b350-acde48001122',
            'a3cdeb0a-d960-11e9-b61b-acde48001122',
          ],
        },
        txn,
      );
      expect(results).toHaveLength(2);
      expect(results.map((m) => m.id).sort()).toEqual([
        'a3cdeb0a-d960-11e9-b61b-acde48001122',
        'a3cef19e-d960-11e9-b350-acde48001122',
      ]);
    });

    it('filters by memberId AND datasource', async () => {
      const results: any = await Member.getMultiple(
        {
          memberIds: [
            'a3cef19e-d960-11e9-b350-acde48001122',
            'a3cdeb0a-d960-11e9-b61b-acde48001122',
          ],
          datasource: 'connecticare',
        },
        txn,
      );
      expect(results).toHaveLength(1);
      expect(results[0].id).toEqual('a3cef19e-d960-11e9-b350-acde48001122');
    });

    it('returns empty for empty externalIds filter', async () => {
      const results = await Member.getMultiple({ externalIds: [] }, txn);
      expect(results).toHaveLength(0);
    });

    it('returns empty for non-existent externalIds', async () => {
      const results = await Member.getMultiple({ externalIds: ['FAKE'] }, txn);
      expect(results).toHaveLength(0);
    });

    it('filters by single externalId', async () => {
      const results: any = await Member.getMultiple({ externalIds: ['C1'] }, txn);
      expect(results).toHaveLength(1);
      expect(results[0].id).toEqual('a3ce52a2-d960-11e9-b350-acde48001122');
    });

    it('filters by multiple externalIds', async () => {
      const results: any = await Member.getMultiple({ externalIds: ['C2', 'M4'] }, txn);
      expect(results).toHaveLength(2);
      expect(results.map((m) => m.id).sort()).toEqual([
        'a3cdeb0a-d960-11e9-b61b-acde48001122',
        'a3cef19e-d960-11e9-b350-acde48001122',
      ]);
    });

    it('filters by externalId AND datasource', async () => {
      const results: any = await Member.getMultiple(
        { externalIds: ['M1', 'C1'], datasource: 'connecticare' },
        txn,
      );
      expect(results).toHaveLength(1);
      expect(results[0].id).toEqual('a3ce52a2-d960-11e9-b350-acde48001122');
    });

    it('works without filters', async () => {
      const results = await Member.getMultiple({}, txn);
      expect(results).toHaveLength(20);
    });
  });

  describe('delete', () => {
    it('throws error on invalid ID', async () => {
      await expect(Member.delete('INVALID', null, null, txn)).rejects.toThrow();
    });

    it('throws error on an id that does not exist', async () => {
      await expect(
        Member.delete('00000000-0000-1000-8000-000000000000', null, null, txn),
      ).rejects.toThrow();
    });

    it('soft deletes a member', async () => {
      const newlyCreatedMember = await Member.create('emblem', 'commons', 1, 2, 'blob', txn);
      const result = await Member.delete(newlyCreatedMember.id, 'time is up', null, txn);
      const memberDatasourceIds = await MemberInsurance.query(txn).where({
        memberId: newlyCreatedMember.id,
      });
      const deletedAtDates = memberDatasourceIds.map((mdi) => mdi.deletedAt);

      expect(result.deletedAt).toBeDefined();
      expect(result.deletedReason).toStrictEqual('time is up');
      expect(deletedAtDates.every((date) => !!date)).toBe(true);
    });

    it('ensures "double delete" does not update the deletedAt date', async () => {
      const newlyCreatedMember = await Member.create('emblem', 'commons', 1, 2, null, txn);
      const { deletedAt: date } = await Member.delete(newlyCreatedMember.id, null, null, txn);
      await sleep(1);
      const result = await Member.delete(newlyCreatedMember.id, null, null, txn);

      expect(result.deletedAt).toStrictEqual(date);
    });
    // TODO: add test for elation deletion failure
    // TODO: add test for zendesk deletion failure
  });

  describe('updateCategory', () => {
    it('should update an empty category to a specified value', async () => {
      const memberWithoutCategory = '0d2e4cc6-e153-11e9-988e-acde48001122';
      const categoryName = 'lower risk';
      const category = await Category.getByName(categoryName, txn);
      const updatedMember = await Member.updateCategory(memberWithoutCategory, categoryName, txn);
      expect(updatedMember.categoryId).toEqual(category.id);
    });
  });

  describe('update', () => {
    // TODO add test for elation update failure
    it('throws error on invalid ID', async () => {
      await expect(Member.update('INVALID', null, txn)).rejects.toThrow();
    });

    it('throws error on an id that does not exist', async () => {
      await expect(
        Member.update('00000000-0000-1000-8000-000000000000', null, txn),
      ).rejects.toThrow();
    });

    it('fully updates a members demographics and insurance info', async () => {
      const { id } = await Member.create('emblem', 'commons', 1, 2, 'testMrn', txn);
      const memberUpdateTest: IMemberUpdate = {
        demographics: {
          firstName: 'Aegon',
          lastName: 'Targaryen',
          dateOfBirth: '07-28-1980',
          isMarkedDeceased: false,
          sex: 'male',
          addresses: [
            {
              street1: '1 Main Street',
              street2: 'Apartment #1',
              city: 'Kings Landing',
              state: 'Westeros',
              zip: '10000',
            },
          ],
          phones: [
            {
              phone: '2139086547',
              phoneType: 'mobile',
            },
          ],
          emails: [
            {
              email: 'aegon@gmail.com',
            },
          ],
          updatedBy: userId,
        },
        partner: 'emblem',
        insurances: [
          {
            carrier: 'emblem',
            plans: [
              {
                externalId: uuid(),
                current: true,
              },
            ],
          },
        ],
        updatedBy: userId,
      };

      await Member.update(id, memberUpdateTest, txn);

      const aegonMemberDemographics = await MemberDemographics.getAllByMemberId(id, txn);
      const aegonMemberAddresses = await Address.getAllByMemberId(id, txn);
      const aegonMemberPhones = await Phone.getAllByMemberId(id, txn);
      const aegonMemberEmails = await Email.getAllByMemberId(id, txn);
      const aegonMemberInsurances = await MemberInsurance.getByMember(id, txn);

      expect(aegonMemberDemographics).toHaveLength(1);
      expect(aegonMemberAddresses).toHaveLength(1);
      expect(aegonMemberPhones).toHaveLength(1);
      expect(aegonMemberEmails).toHaveLength(1);
      expect(aegonMemberInsurances).toHaveLength(1);

      expect(aegonMemberDemographics[0]).toMatchObject({
        firstName: 'Aegon',
        lastName: 'Targaryen',
        dateOfBirth: '07-28-1980',
        sex: 'male',
        updatedBy: userId,
      });

      expect(aegonMemberAddresses[0]).toMatchObject({
        street1: '1 Main Street',
        street2: 'Apartment #1',
        city: 'Kings Landing',
        state: 'Westeros',
        zip: '10000',
      });

      expect(aegonMemberPhones[0]).toMatchObject({
        phone: '2139086547',
        phoneType: 'mobile',
      });

      expect(aegonMemberEmails[0]).toMatchObject({
        email: 'aegon@gmail.com',
      });

      expect(aegonMemberInsurances[0]).toMatchObject({
        carrier: 'emblem',
        plans: [
          {
            externalId: expect.any(String),
            current: true,
          },
        ],
      });
    });

    it('updates a members zendesk id', async () => {
      const zendeskId = '419499297374';
      const { id } = await Member.create('emblem', 'commons', 1, 2, 'testMrn', txn);

      const memberUpdateTest: IMemberUpdate = { zendeskId };
      const member = await Member.update(id, memberUpdateTest, txn);

      expect(member.zendeskId).toBe(zendeskId);
    });
  });
});
