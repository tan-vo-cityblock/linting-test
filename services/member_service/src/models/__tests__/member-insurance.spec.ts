import { flatten, values } from 'lodash';
import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { IInsurancePlan, IInsurance, MemberInsurance } from '../member-insurance';
import { MemberInsuranceDetails } from '../member-insurance-details';
import { expectedAllMapping, expectedMemberFilterMapping, expectedPartnerMapping } from './test-data';

describe('Member Insurance model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  it('successfully creates an insuranceID for an existing member', async () => {
    const expected = [
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        externalId: 'E5',
        datasourceId: 3,
        current: true,
      },
    ];
    const request: IInsurance[] = [
      { carrier: 'connecticare', plans: [{ externalId: 'E5', current: true }] },
    ];
    const result = await MemberInsurance.createInsurances(
      'a3ccab32-d960-11e9-b350-acde48001122',
      request,
      txn,
    );
    const slimResult = flatten(result).map((mdi) =>
      Object.assign(mdi, { updatedAt: undefined, createdAt: undefined, id: undefined }),
    );
    expect(slimResult).toMatchObject(expected);
  });

  it('successfully creates multiple insurance plans with multiple carriers', async () => {
    const expected = [
      {
        current: true,
        datasourceId: 1,
        externalId: 'newInsurance',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        rank: 'primary',
        details: [
          {
            spanDateStart: new Date('2020-01-01T05:00:00.000Z'),
          },
        ],
      },
      {
        current: false,
        datasourceId: 1,
        externalId: 'oldInsurance',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        rank: 'primary',
        details: [
          {
            spanDateEnd: new Date('2020-01-01T05:00:00.000Z'),
            spanDateStart: new Date('2019-01-01T05:00:00.000Z'),
          },
        ],
      },
      {
        datasourceId: 3,
        externalId: 'testing',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        rank: 'secondary',
      },
    ];

    const request: IInsurance[] = [
      {
        carrier: 'emblem',
        plans: [
          {
            externalId: 'newInsurance',
            rank: 'primary',
            current: true,
            details: [
              {
                lineOfBusiness: 'medicaid',
                subLineOfBusiness: 'medicaid',
                spanDateStart: '2020-01-01',
              },
            ],
          },
          {
            externalId: 'oldInsurance',
            rank: 'primary',
            current: false,
            details: [
              {
                lineOfBusiness: 'commercial',
                subLineOfBusiness: 'exchange',
                spanDateStart: '2019-01-01',
                spanDateEnd: '2020-01-01',
              },
            ],
          },
        ],
      },
      {
        carrier: 'connecticare',
        plans: [
          {
            externalId: 'testing',
            rank: 'secondary',
            details: [],
          },
        ],
      },
    ];
    const result = await MemberInsurance.createInsurances(
      'a74bcb4a-d964-11e9-8372-acde48001122',
      request,
      txn,
    );

    const slimResult = flatten(values(result)).map((mdi) => {
      return { ...mdi, updatedAt: undefined, createdAt: undefined, id: undefined };
    });
    expect(slimResult).toMatchObject(expected);
  });

  it('sucessfully soft deletes an insurance ID for a member', async () => {
    const memberId = '0d2e4cc6-e153-11e9-988e-acde48001122';
    const insuranceId = 'B1';
    const deletedReason = 'testing deletion on model';
    const deletedInsurance = await MemberInsurance.deleteByInsurance(
      memberId,
      insuranceId,
      deletedReason,
      txn,
    );
    expect(deletedInsurance).toBeDefined();
    expect(deletedInsurance.deletedAt).toBeDefined();
    expect(deletedInsurance.deletedReason).toEqual('testing deletion on model');
    expect(deletedInsurance.memberId).toEqual(memberId);
    expect(deletedInsurance.deletedReason).toEqual(deletedReason);
  });

  it('successfully updates a members insurance info', async () => {
    const expected = [
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        externalId: 'M1',
        datasourceId: 1,
        deletedAt: null,
        deletedReason: null,
        current: false,
        rank: 'primary',
      },
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        externalId: 'M2',
        datasourceId: 1,
        deletedAt: null,
        deletedReason: null,
        current: true,
        rank: 'secondary',
      },
    ];

    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const carrier = 'emblem';
    const plans: IInsurancePlan[] = [
      {
        externalId: 'M1',
        rank: 'primary',
        current: false,
      },
      {
        externalId: 'M2',
        rank: 'secondary',
        current: true,
      },
    ];

    const result = await MemberInsurance.updateInsurances(
      { memberId, carrier, plans },
      'Haris',
      txn,
    );
    expect(result).toMatchObject(expected);
  });

  it('does not update insurance details for a member', async () => {
    const expectedUpdatedDetails = {
      lineOfBusiness: 'commercial',
      memberDatasourceId: 'c514831a-940d-45d4-b824-72d46edabb1a',
      spanDateStart: new Date('2019-01-01T05:00:00.000Z'),
      spanDateEnd: new Date('2020-01-01T05:00:00.000Z'),
      subLineOfBusiness: null,
    };

    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const carrier = 'emblem';
    const plans: IInsurancePlan[] = [
      {
        externalId: 'M1',
        details: [
          {
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'medicaid',
            spanDateStart: '2018-12-31',
            spanDateEnd: '2019-12-31',
          },
        ],
      },
    ];

    const result = await MemberInsurance.updateInsurances(
      { memberId, carrier, plans },
      'Haris',
      txn,
    );

    expect(result).toMatchObject([{}]);

    const details = await MemberInsuranceDetails.query(txn).where({
      memberDatasourceId: 'c514831a-940d-45d4-b824-72d46edabb1a',
      deletedAt: null,
    });

    expect(details).toContainEqual(expect.objectContaining(expectedUpdatedDetails));
  });

  it('successfully gets insurance info with respect to a member', async () => {
    const result: IInsurance[] = await MemberInsurance.getByMember(
      'a3ccab32-d960-11e9-b350-acde48001122',
      txn,
    );

    expect(result).toBeInstanceOf(Array); // It should not be an instance of Lodash Wrapper

    const emblemPlans = result.find((insurance) => insurance.carrier === 'emblem').plans;
    const medicaidPlans = result.find((insurance) => insurance.carrier === 'medicaidNY').plans;

    expect(emblemPlans).toHaveLength(3);

    expect(emblemPlans).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          externalId: 'M1',
          details: expect.arrayContaining([
            expect.objectContaining({ spanDateStart: new Date('2019-01-01T05:00:00.000Z') }),
            expect.objectContaining({ spanDateStart: new Date('2020-03-01T05:00:00.000Z') }),
            expect.objectContaining({ spanDateStart: new Date('2020-12-01T05:00:00.000Z') }),
          ]),
        }),
        expect.objectContaining({ externalId: 'M2', details: expect.arrayContaining([
            expect.objectContaining({ spanDateStart: new Date('2021-01-01T05:00:00.000Z') }),
          ])
        }),
        expect.objectContaining({ externalId: 'Z3', details: [] }),
      ]),
    );

    expect(medicaidPlans).toHaveLength(1);
    expect(medicaidPlans).toEqual(
      expect.arrayContaining([expect.objectContaining({ externalId: 'CIN' })]),
    );
  });

  it('successfully creates insurance plans for tufts for either enrollment form or regular batch loading', async () => {
    const expected = [
      {
        datasourceId: 9,
        externalId: 'newInsurance',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        details: [
          {
            lineOfBusiness: 'commercial',
            subLineOfBusiness: 'direct',
            spanDateEnd: null,
            spanDateStart: null,
          },
        ],
      },
      {
        datasourceId: 9,
        externalId: 'oldInsurance',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        details: [
          {
            lineOfBusiness: 'medicaid',
            subLineOfBusiness: 'MCO',
            spanDateEnd: null,
            spanDateStart: null,
          },
        ],
      },
      {
        datasourceId: 9,
        externalId: 'testing',
        memberId: 'a74bcb4a-d964-11e9-8372-acde48001122',
        details: [
          {
            lineOfBusiness: 'medicare',
            subLineOfBusiness: 'dual',
            spanDateEnd: null,
            spanDateStart: null,
          },
        ],
      },
    ];

    const enrollmentFormRequest: IInsurance[] = [
      {
        carrier: 'tufts',
        plans: [
          {
            externalId: 'newInsurance',
            details: [
              {
                lineOfBusiness: 'commercial',
                subLineOfBusiness: 'direct',
              },
            ],
          },
          {
            externalId: 'oldInsurance',
            details: [
              {
                lineOfBusiness: 'medicaid',
                subLineOfBusiness: 'MCO',
              },
            ],
          },
          {
            externalId: 'testing',
            details: [
              {
                lineOfBusiness: 'medicare',
                subLineOfBusiness: 'dual',
              },
            ],
          },
        ],
      },
    ];

    const result = await MemberInsurance.createInsurances(
      'a74bcb4a-d964-11e9-8372-acde48001122',
      enrollmentFormRequest,
      txn,
    );

    const slimResult = flatten(values(result)).map((mdi) => {
      return { ...mdi, updatedAt: undefined, createdAt: undefined, id: undefined };
    });
    expect(slimResult).toMatchObject(expected);
  });

  it('successfully creates a medicaid and medicare ID for an existing member', async () => {
    const expected = [
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        externalId: '678Hgsy',
        datasourceId: 27,
        details: [],
      },
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        datasourceId: 26,
        externalId: 'DFG75432P',
        details: [],
      },
    ];

    const requests: IInsurance[] = [
      { carrier: 'medicareCT', plans: [{ externalId: '678Hgsy' }] },
      { carrier: 'medicaidCT', plans: [{ externalId: 'DFG75432P' }] },
    ];

    const result = await Promise.all(
      requests.map(async (request) =>
        MemberInsurance.createInsurance(
          'a3ccab32-d960-11e9-b350-acde48001122',
          request.carrier,
          request.plans,
          txn,
        ),
      ),
    );

    expect(flatten(result)).toMatchObject(expected);
  });

  it('successfully updates a members medicare and medicaid info using insurance method', async () => {
    const expected = [
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        externalId: 'CIN',
        datasourceId: 8,
        deletedAt: null,
        deletedReason: null,
        current: true,
        rank: 'primary',
      },
    ];

    const request: IInsurance = {
      carrier: 'medicaidNY',
      plans: [
        {
          externalId: 'CIN',
          rank: 'primary',
          current: true,
        },
      ],
    };

    const result = await MemberInsurance.updateInsurances(
      {
        memberId: 'a3ccab32-d960-11e9-b350-acde48001122',
        carrier: request.carrier,
        plans: request.plans,
      },
      null,
      txn,
    );

    const slimResult = result.map((x) => {
      return { ...x, id: undefined, createdAt: undefined, updatedAt: undefined };
    });
    expect(slimResult).toMatchObject(expected);
  });

  describe('getMemberInsuranceMapping', () => {
    it('successfully gets member insurance mapping for members', async () => {
      const memberMapping = await MemberInsurance.getMemberInsuranceMapping({}, txn);
      expect(memberMapping).toMatchObject(expect.arrayContaining(expectedAllMapping));
    });

    it('successfully gets member insurance mapping for members based on partners', async () => {
      const memberMapping = await MemberInsurance.getMemberInsuranceMapping(
        { carrier: 'connecticare' },
        txn,
      );

      expect(memberMapping).toMatchObject(expect.arrayContaining(expectedPartnerMapping));
      expect(memberMapping.length).toBe(3);
    });

    it('successfully fetches a member insurance mapping based on their externalId and carrier', async () => {
      const memberMapping = await MemberInsurance.getMemberInsuranceMapping(
        { carrier: 'emblem', externalId: 'M3' },
        txn,
      );

      expect(memberMapping).toMatchObject(expect.arrayContaining(expectedMemberFilterMapping));
      expect(memberMapping.length).toBe(1);
    });

    it('returns null for a member that does not have a corresponding externalId, carrier', async () => {
      const memberMapping = await MemberInsurance.getMemberInsuranceMapping(
        { carrier: 'emblem', externalId: '007' },
        txn,
      );

      expect(memberMapping).toMatchObject([]);
    });
  });

  describe('createOrAppendInsuranceDetails', () => {
    describe('when a member insurance already exists', () => {
      it('adds insurance details', async () => {
        const appendInsuranceDetailsSpy = jest.spyOn(MemberInsuranceDetails, 'append');
        const createInsuranceSpy = jest.spyOn(MemberInsurance, 'createInsurance');

        const insuranceDetailsSeq = await MemberInsuranceDetails.getByExternalId(
          'M1',
          'emblem',
          txn,
        );

        expect(insuranceDetailsSeq).toHaveLength(3);

        const newInsuranceDetails = {
          externalId: 'M1',
          details: [
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
          ],
        };
        await MemberInsurance.createOrAppendInsuranceAndDetails(
          'a3ccab32-d960-11e9-b350-acde48001122',
          'emblem',
          newInsuranceDetails,
          txn,
        );

        expect(appendInsuranceDetailsSpy).toHaveBeenCalledTimes(1);
        expect(appendInsuranceDetailsSpy).toHaveBeenCalledWith('emblem', newInsuranceDetails, txn);
        expect(createInsuranceSpy).toHaveBeenCalledTimes(0);
      });
    });

    describe('when a member insurance does not already exist', () => {
      it('creates a member insurance and adds insurance details', async () => {
        const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
        const carrier = 'emblem';
        const externalId = 'BRAND_NEW';
        const newInsuranceDetails = {
          externalId,
          details: [
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: 'medicare',
              spanDateStart: '2020-11-01',
              spanDateEnd: '2020-12-01',
            },
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: 'medicare',
              spanDateStart: '2021-01-01',
              spanDateEnd: '2021-02-01',
            },
          ],
        };
        const memberInsurances = await MemberInsurance.getInsurancesByDatasource(
          memberId,
          carrier,
          txn,
        );
        const memberExternalIds = memberInsurances.map((insurance) => insurance.externalId);

        expect(memberExternalIds).toHaveLength(3);
        expect(memberExternalIds).not.toContain(externalId);

        await MemberInsurance.createOrAppendInsuranceAndDetails(
          memberId,
          carrier,
          newInsuranceDetails,
          txn,
        );

        const refetchedMemberInsurances = await MemberInsurance.getInsurancesByDatasource(
          memberId,
          carrier,
          txn,
        );
        const refetchedMemberExternalIds = refetchedMemberInsurances.map(
          (insurance) => insurance.externalId,
        );

        expect(refetchedMemberExternalIds).toHaveLength(4);
        expect(refetchedMemberExternalIds).toContain(externalId);
      });
    });
  });

  describe('getLatestInsurance', () => {
    it('returns the latest commercial insurance for a member if one exists', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const latestMemberInsurance = await MemberInsurance.getLatestInsurance(memberId, txn);
      expect(latestMemberInsurance).toMatchObject(
        expect.objectContaining(
          {
            carrier: 'emblem',
            plans: expect.arrayContaining([
              expect.objectContaining({
                externalId: 'M2',
                rank: null,
                current: true,
                details: expect.arrayContaining([
                  expect.objectContaining({
                    spanDateStart: new Date('2021-01-01T05:00:00.000Z'),
                    spanDateEnd: null,
                    lineOfBusiness: 'medicare',
                    subLineOfBusiness: null
                  })
                ])
              })
            ])
          }
        )
      );
    });

    it('returns null if a member has no active insurance', async () => {
      const memberId = 'a3cdcb8e-d960-11e9-b350-acde48001122';
      const latestMemberInsurance = await MemberInsurance.getLatestInsurance(memberId, txn);
      expect(latestMemberInsurance).toBeNull();
    });
  });
});
