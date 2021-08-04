import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import {
  IMemberDemographics,
  IUpdateMemberDemographics,
  MemberDemographics,
} from '../member-demographics';
import { processAddresses, processEmails, processPhones } from '../member-demographics';
import { queriedMemberFromDatabase } from './test-data';

describe('Member Demographics model', () => {
  let memberId: string;
  let memberDemographics: IMemberDemographics;
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    memberDemographics = {
      firstName: 'Terry',
      lastName: 'Platypus',
      dateOfBirth: '1980-01-01',
      isMarkedDeceased: false,
      sex: 'male',
      addresses: [],
      phones: [],
      emails: [],
    };
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
    memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
  });

  afterEach(async () => {
    await txn.rollback();
  });

  describe('create', () => {
    it('creates a member demographics record for a member', async () => {
      const memberDemographicsRecord = await MemberDemographics.create(
        memberId,
        memberDemographics,
        txn,
      );
      expect(memberDemographicsRecord).toMatchObject({
        firstName: 'Terry',
        lastName: 'Platypus',
        dateOfBirth: '1980-01-01',
        sex: 'male',
        maritalStatus: 'unknown',
      });
    });

    it('returns null when passed an empty demographics object', async () => {
      const demographics: IMemberDemographics = {};
      const result: MemberDemographics = await MemberDemographics.create(
        memberId,
        demographics,
        txn,
      );
      expect(result).toBeNull();
    });

    it('should throw an exception, if required fields are null', async () => {
      const demographics: IMemberDemographics = {
        firstName: 'Perry',
        lastName: null,
        dateOfBirth: '1980-01-01',
        sex: 'male',
        isMarkedDeceased: false,
      };

      try {
        expect.assertions(1);
        await MemberDemographics.create(memberId, demographics, txn);
      } catch (e) {
        expect(e.message.split(' - ')[1]).toEqual(
          'null value in column "lastName" violates not-null constraint',
        );
      }
    });
  });

  describe('get', () => {
    it('gets latest member demographics record for a member', async () => {
      await MemberDemographics.create(memberId, memberDemographics, txn);
      const memberDemographicsRecord = await MemberDemographics.getLatestByMemberId(memberId, txn);
      expect(memberDemographicsRecord).toMatchObject({
        firstName: 'Terry',
        lastName: 'Platypus',
        dateOfBirth: '1980-01-01',
        sex: 'male',
      });
    });
  });

  describe('update', () => {
    it('updates a member demographics record for a member', async () => {
      await MemberDemographics.create(memberId, memberDemographics, txn);

      const updateUser = '5d2af62f-1c75-408d-81b7-0709d6d9842d';
      const updatedDemographics: IUpdateMemberDemographics = {
        firstName: 'Terry2',
        lastName: 'Platypus2',
        updatedBy: updateUser,
      };

      await MemberDemographics.updateAndSaveHistory(memberId, updatedDemographics, txn);
      const memberDemographicsArray = await MemberDemographics.getAllByMemberId(memberId, txn);

      expect(memberDemographicsArray).toHaveLength(2);

      const currentMemberDemographics = memberDemographicsArray.filter((md) => !md.deletedAt)[0];
      const previousMemberDemographics = memberDemographicsArray.filter((md) => !!md.deletedAt)[0];

      expect(previousMemberDemographics).toMatchObject({
        firstName: 'Terry',
        lastName: 'Platypus',
      });
      expect(currentMemberDemographics).toMatchObject({
        firstName: 'Terry2',
        lastName: 'Platypus2',
        updatedBy: updateUser,
      });
    });

    it('updates a member demographics record for a member with no prior history', async () => {
      const updateUser = '5d2af62f-1c75-408d-81b7-0709d6d9842d';
      const updatedDemographics: IUpdateMemberDemographics = {
        firstName: 'Terry2',
        lastName: 'Platypus2',
        updatedBy: updateUser,
        dateOfBirth: '10-08-1971',
      };

      await MemberDemographics.updateAndSaveHistory(memberId, updatedDemographics, txn);
      const memberDemographicsArray = await MemberDemographics.getAllByMemberId(memberId, txn);

      expect(memberDemographicsArray).toHaveLength(1);

      const currentMemberDemographics = memberDemographicsArray.filter((md) => !md.deletedAt)[0];

      expect(currentMemberDemographics).toMatchObject({
        firstName: 'Terry2',
        lastName: 'Platypus2',
        dateOfBirth: '10-08-1971',
        updatedBy: updateUser,
      });
    });
  });

  describe('get', () => {
    it('checks that phones are correctly mapped from a nested array to a phone object', async () => {
      const processedPhones = processPhones(queriedMemberFromDatabase.phones);
      expect(processedPhones).toEqual([
        {
          id: '392fb165-8715-4832-8ade-b1d58d56c6b8',
          phone: '7185526554',
          phoneType: 'main',
        },
        {
          id: '0664695e-55a2-411b-8f81-b94fcf806d08',
          phone: '5182329951',
          phoneType: 'main',
        },
      ]);
    });
  });

  it('checks that addresses are correctly mapped from a nested array to an address object', async () => {
    const processedAddresses = processAddresses(queriedMemberFromDatabase.addresses);
    expect(processedAddresses).toEqual([
      {
        id: '9f777b6f-e32b-4a1e-bac7-49d6d7c9dfaf',
        addressType: null,
        street1: '25 Rugby Road',
        street2: null,
        county: null,
        city: 'Brooklyn',
        state: 'NY',
        zip: '11223',
        spanDateStart: null,
        spanDateEnd: null,
      },
    ]);
  });

  it('checks that emails are correctly mapped from a nested array to an email object', async () => {
    const processedEmails = processEmails(queriedMemberFromDatabase.emails);
    expect(processedEmails).toEqual([
      {
        id: 'cea58957-d96e-4d46-9a2c-a2ffeb96347a',
        email: 'bob@live.com',
      },
    ]);
  });
});
