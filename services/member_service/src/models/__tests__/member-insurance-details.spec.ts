import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { IInsurancePlan, MemberInsurance } from '../member-insurance';
import { MemberInsuranceDetails } from '../member-insurance-details';

describe('MemberInsuranceDetails model', () => {
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

  describe('update', () => {
    describe('with valid ids', () => {
      it('updates insurance detail information', async () => {
        const insuranceDetailsId = 'd3bce3f6-a938-439a-9139-a36ca4976f2b';
        const insuranceDetails = await MemberInsuranceDetails.query(txn).findById(
          insuranceDetailsId,
        );

        expect(insuranceDetails.lineOfBusiness).toBe('commercial');
        expect(insuranceDetails.subLineOfBusiness).toBeNull();
        expect(insuranceDetails.spanDateStart).toEqual(new Date('2019-01-01T05:00:00.000Z'));
        expect(insuranceDetails.spanDateEnd).toEqual(new Date('2020-01-01T05:00:00.000Z'));

        await MemberInsuranceDetails.update(
          {
            id: insuranceDetailsId,
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'exchange',
            spanDateStart: '2020-12-01',
            spanDateEnd: '2021-02-01',
          },
          txn,
        );

        const {
          lineOfBusiness,
          subLineOfBusiness,
          spanDateStart,
          spanDateEnd,
        } = await MemberInsuranceDetails.query(txn).findById(insuranceDetailsId);
        expect(lineOfBusiness).toEqual('commercial');
        expect(subLineOfBusiness).toEqual('exchange');
        expect(spanDateStart).toEqual(new Date('2020-12-01T05:00:00.000Z'));
        expect(spanDateEnd).toEqual(new Date('2021-02-01T05:00:00.000Z'));
      });
    });

    describe('with invalid ids', () => {
      it('throws an error', async () => {
        const badUpdatedDetails = {
          id: 'not-a-uuid',
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'test-slob',
          spanDateStart: '2020-12-01',
          spanDateEnd: '2021-01-01',
        };

        await expect(MemberInsuranceDetails.update(badUpdatedDetails, txn)).rejects.toThrowError(
          Error('attemping to update member insurance details with invalid id [id: not-a-uuid]'),
        );
      });
    });
  });

  describe('append', () => {
    it('appends a single insurance detail for a member with no details', async () => {
      const expected = [
        {
          memberDatasourceId: 'ba45df7b-f440-42bd-b95b-6451de4bcb68',
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2019-12-31T05:00:00.000Z'),
          spanDateEnd: new Date('2020-12-31T05:00:00.000Z'),
        },
      ];

      const details = [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2019-12-31',
          spanDateEnd: '2020-12-31',
        },
      ];

      const updatedPlan: IInsurancePlan = { externalId: 'M3', details };
      const result = await MemberInsuranceDetails.append('emblem', updatedPlan, txn);
      expect(result).toMatchObject(expected);
    });

    it('appends a single insurance detail for a member with no details while changing rank/current', async () => {
      const expected = [
        {
          memberDatasourceId: 'ba45df7b-f440-42bd-b95b-6451de4bcb68',
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2019-12-31T05:00:00.000Z'),
          spanDateEnd: new Date('2020-12-31T05:00:00.000Z'),
        },
      ];

      const details = [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2019-12-31',
          spanDateEnd: '2020-12-31',
        },
      ];

      const updatedPlan: IInsurancePlan = { externalId: 'M3', rank: "secondary", current: false, details };
      const resultDetails = await MemberInsuranceDetails.append('emblem', updatedPlan, txn);
      const resultInsurance = await MemberInsurance.getByExternalId('M3', 'emblem', txn);
      expect(resultDetails).toMatchObject(expected);
      expect(resultInsurance).toMatchObject({ rank: 'secondary', current: false });
    });

    it('appends multiple insurance details for a member with no details', async () => {
      const expected = [
        {
          memberDatasourceId: '778dc534-3b1f-11eb-9022-a683e7329d29',
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2019-12-31T05:00:00.000Z'),
          spanDateEnd: new Date('2020-12-31T05:00:00.000Z'),
        },
        {
          memberDatasourceId: '778dc534-3b1f-11eb-9022-a683e7329d29',
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2018-12-31T05:00:00.000Z'),
          spanDateEnd: new Date('2019-12-31T05:00:00.000Z'),
        },
      ];

      const details = [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2019-12-31',
          spanDateEnd: '2020-12-31',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2018-12-31',
          spanDateEnd: '2019-12-31',
        },
      ];

      const updatedPlan: IInsurancePlan = { externalId: 'Z3', details };
      const result = await MemberInsuranceDetails.append('emblem', updatedPlan, txn);
      expect(result).toMatchObject(expected);
    });

    it('appends multiple insurance details for a existing member with existing details', async () => {
      const insuranceDetailsSeq = await MemberInsuranceDetails.getByExternalId('M1', 'emblem', txn);

      expect(insuranceDetailsSeq).toHaveLength(3);

      const newInsuranceDetails = [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-04-01',
          spanDateEnd: '2020-05-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-06-01',
          spanDateEnd: '2020-07-01',
        },
      ];

      const updatedPlan: IInsurancePlan = { externalId: 'M1', details: newInsuranceDetails };
      await MemberInsuranceDetails.append('emblem', updatedPlan, txn);

      const refetchedInsuranceDetailsSeq = await MemberInsuranceDetails.getByExternalId(
        'M1',
        'emblem',
        txn,
      );
      const mappedSpans = refetchedInsuranceDetailsSeq.map((details) => ({
        spanDateStart: details.spanDateStart,
        spanDateEnd: details.spanDateEnd,
      }));

      expect(refetchedInsuranceDetailsSeq).toHaveLength(5);
      expect(mappedSpans).toContainEqual({
        spanDateStart: new Date('2020-04-01T04:00:00.000Z'),
        spanDateEnd: new Date('2020-05-01T04:00:00.000Z'),
      });
      expect(mappedSpans).toContainEqual({
        spanDateStart: new Date('2020-06-01T04:00:00.000Z'),
        spanDateEnd: new Date('2020-07-01T04:00:00.000Z'),
      });
    });
  });

  describe('bulkDelete', () => {
    describe('with valid ids', () => {
      it('deletes insurance details', async () => {
        const insuranceDetailsId1 = 'd3bce3f6-a938-439a-9139-a36ca4976f2b';
        const insuranceDetailsId2 = '6e1aca7c-2be7-42ad-a36e-d0d1b8b8e7fe';
        const insuranceDetailsId3 = '40928c49-04db-449c-bb46-d28e34f7912a';
        const insuranceDetailsSeq = await MemberInsuranceDetails.getByExternalId(
          'M1',
          'emblem',
          txn,
        );

        expect(insuranceDetailsSeq).toHaveLength(3);

        await MemberInsuranceDetails.bulkDelete([insuranceDetailsId1, insuranceDetailsId3], txn);

        const refetchedInsuranceDetailsSeq = await MemberInsuranceDetails.getByExternalId(
          'M1',
          'emblem',
          txn,
        );

        expect(refetchedInsuranceDetailsSeq).toHaveLength(1);
        expect(refetchedInsuranceDetailsSeq[0].id).toEqual(insuranceDetailsId2);
      });
    });

    describe('with invalid ids', () => {
      it('throws an error', async () => {
        await expect(
          MemberInsuranceDetails.bulkDelete(
            ['40928c49-04db-449c-bb46-d28e34f7912a', 'not-a-uuid'],
            txn,
          ),
        ).rejects.toThrowError(
          Error(
            'attemping to delete member insurance details with invalid id [ids: 40928c49-04db-449c-bb46-d28e34f7912a,not-a-uuid]',
          ),
        );
      });
    });
  });
});
