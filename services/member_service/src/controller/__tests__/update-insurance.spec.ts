import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { IInsurancePlan } from '../../models/member-insurance';
import { IInsuranceDetails, MemberInsuranceDetails } from '../../models/member-insurance-details';
import { IUpdateInsuranceResponse, updateInsurance } from '../update-insurance';
import {
  updateDetailsForMultipleExternalIdsRequest,
  updateDetailsForOneExternalIdRequest,
  updateDetailsForOneExternalIdGapAdditionRequest,
  updateDetailsForOneExternalIdHistoryChangeRequest,
  updateDetailsFromOverlappingSpansRequest,
  updateDetailsFromOverlappingSpansForMultipleExternalIdsRequest,
  updateDetailsGapAdditionRequest,
  updateDetailsGapRemovalRequest,
  updateDetailsDateAndLobSlobChangeRequest,
  updateDetailsLobSlobChangeRequest,
  updateDetailsNoChangeRequest,
  updateMultipleDetailsRequest,
  updateMultipleDetailsForTwoExternalIdsRequest,
  updateSingleDetailsRequest,
  updateSingleDetails3YearsOldRequest
} from './test-data';
import { setupDb } from './test-setup';
import { flatten } from 'lodash';

interface ITestUpdateInsurancesResult {
  statusCode: number;
  data: IUpdateInsuranceResponse;
  detailsSeq: IInsuranceDetails[];
}

async function testUpdateInsurances(memberId: string, requestBody: any, txn: Transaction): Promise<ITestUpdateInsurancesResult> {
  const response = httpMocks.createResponse();
  const request = httpMocks.createRequest({
    method: 'PATCH',
    url: `/${API_VERSION}/members/${memberId}/insurance`,
    params: { memberId },
    body: requestBody,
  });
  request.txn = txn;
  await updateInsurance(request, response);

  const data = response._getData()
  const { carrier, plans } = requestBody;
  const { statusCode } = response;
  const detailsSeq = flatten(
    await Promise.all(
      plans.map(async (plan: IInsurancePlan) => {
        const details = await MemberInsuranceDetails.getByExternalId(plan.externalId, carrier, txn);
        return details.map((detail: IInsuranceDetails) => (
          { externalId: plan.externalId, ...detail }
        ))
      })
    )
  );

  return { statusCode, data, detailsSeq };
}

describe('Update insurance route', () => {
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  describe('requests containing a single external id', () => {
    describe('with 1 existing details and 1 details in request', () => {
      it('updates 1 details in the database', async () => {
        const memberId = '6d4c1ad4-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateSingleDetailsRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M8',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: new Date('2021-02-01T05:00:00.000Z'),
          spanDateEnd: new Date('2021-03-01T05:00:00.000Z'),
        }));
      });
    });

    describe('with 2 existing details and 2 details in request', () => {
      it('updates 2 details in the database', async () => {
        const memberId = '913cd51e-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateMultipleDetailsRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M9',
              detailsCreated: 0,
              detailsUpdated: 2,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
            }),
            expect.objectContaining({
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: new Date('2021-03-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
          ]),
        );
      });
    });

    describe('with 2 existing details and 1 details in request', () => {
      it('updates 1 details and deletes 1 details in the database', async () => {
        const memberId = '92b58a6c-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsGapRemovalRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M10',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 1,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
          spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
        }))
      });
    });

    describe('with 1 existing details and 2 details in request', () => {
      it('creates 1 details in the database and does not update any details', async () => {
        const memberId = '93560f14-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsGapAdditionRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M11',
              detailsCreated: 1,
              detailsUpdated: 0,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
            }),
            expect.objectContaining({
              lineOfBusiness: null,
              subLineOfBusiness: null,
              spanDateStart: new Date('2021-03-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
          ]),
        );
      });
    });

    describe('with 1 existing details and 1 details with new lob/slob in request', () => {
      it('creates 1 details and deletes 1 details in the database', async () => {
        const memberId = '93c51576-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsLobSlobChangeRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M12',
              detailsCreated: 1,
              detailsUpdated: 0,
              detailsDeleted: 1,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
          spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
        }))
      });
    });

    describe('with 2 existing details and 1 details with updated span date and 1 details with updated lob/slob in request', () => {
      it('updates 1 details, creates 1 details, and deletes 1 details in the database', async () => {
        const memberId = '942164d4-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsDateAndLobSlobChangeRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M13',
              detailsCreated: 1,
              detailsUpdated: 1,
              detailsDeleted: 1,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              lineOfBusiness: 'medicare',
              subLineOfBusiness: 'medicare',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2020-12-01T05:00:00.000Z'),
            }),
            expect.objectContaining({
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2020-12-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-03-01T05:00:00.000Z'),
            }),
          ]),
        );
      });
    });

    describe('with 1 existing details and 1 identical details in request', () => {
      it('does not update, create, or delete a details in the database', async () => {
        const memberId = '94767a50-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsNoChangeRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M14',
              detailsCreated: 0,
              detailsUpdated: 0,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
          spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
        }))
      });
    });

    describe('with 1 existing details and 1 details in request', () => {
      it('updates only the span end date in the database if the database details is older than 3 years ago', async () => {
        const memberId = '54e1d2d0-61b4-11eb-a3a5-3af9d391d31e';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateSingleDetails3YearsOldRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M24',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: new Date('2017-06-01T04:00:00.000Z'),
          spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
        }));
      });
    });

    describe('reducing request details', () => {
      it('consolidates overlapping spans', async () => {
        const memberId = '7956188e-5f5b-11eb-80d7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsFromOverlappingSpansRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M21',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(1);
        expect(detailsSeq).toContainEqual(expect.objectContaining({
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
          spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
        }))
      });
    });
  });

  describe('requests containing multiple external ids', () => {
    describe('with 1 existing details for each external id', () => {
      it('updates details for both external ids', async () => {
        const memberId = '94e15c62-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsForMultipleExternalIdsRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M15',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
            {
              externalId: 'M16',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M16',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-05-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-06-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M15',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
          ]),
        );
      });

      it('updates details for only one external id', async () => {
        const memberId = '94e15c62-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsForOneExternalIdRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M15',
              detailsCreated: 0,
              detailsUpdated: 0,
              detailsDeleted: 0,
            },
            {
              externalId: 'M16',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M16',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-02-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M15',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-01-01T05:00:00.000Z'),
            }),
          ]),
        );
      });

      it('updates details for one external id and creates details for another external id', async () => {
        const memberId = '94e15c62-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsForOneExternalIdGapAdditionRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M15',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
            {
              externalId: 'M16',
              detailsCreated: 1,
              detailsUpdated: 0,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(3);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M16',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-01-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M16',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-05-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-06-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M15',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
            }),
          ]),
        );
      });

      it('updates details for one external id and updates and deletes details for another external id', async () => {
        const memberId = '95c8a766-5cfe-11eb-9dd7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsForOneExternalIdHistoryChangeRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M17',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
            {
              externalId: 'M18',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 1,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M17',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M18',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-01-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-06-01T04:00:00.000Z'),
            }),
          ]),
        );
      });
    });

    describe('with multiple existing details for each external id', () => {
      it('it updates details for both external ids, creates details for one external id, and deletes details for the other', async () => {
        const memberId = '79c931de-5f5b-11eb-80d7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateMultipleDetailsForTwoExternalIdsRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M19',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 1,
            },
            {
              externalId: 'M20',
              detailsCreated: 1,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(4);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M19',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-10-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-05-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M20',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2020-08-01T04:00:00.000Z'),
              spanDateEnd: new Date('2020-11-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M20',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-01-01T05:00:00.000Z'),
              spanDateEnd: new Date('2021-04-01T04:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M20',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2021-05-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-06-01T04:00:00.000Z'),
            }),
          ]),
        );
      });
    });

    describe('reducing request details', () => {
      it('consolidates overlapping spans', async () => {
        const memberId = '7890a914-5f5b-11eb-80d7-acde48001122';
        const { data, statusCode, detailsSeq } = await testUpdateInsurances(memberId, updateDetailsFromOverlappingSpansForMultipleExternalIdsRequest, txn);

        expect(statusCode).toBe(200);
        expect(data).toMatchObject({
          updatedInsuranceStatuses: [
            {
              externalId: 'M22',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
            {
              externalId: 'M23',
              detailsCreated: 0,
              detailsUpdated: 1,
              detailsDeleted: 0,
            },
          ],
        });
        expect(detailsSeq).toHaveLength(2);
        expect(detailsSeq).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              externalId: 'M23',
              lineOfBusiness: 'commercial',
              subLineOfBusiness: 'exchange',
              spanDateStart: new Date('2020-10-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-01-01T05:00:00.000Z'),
            }),
            expect.objectContaining({
              externalId: 'M22',
              lineOfBusiness: 'medicaid',
              subLineOfBusiness: 'medicaid',
              spanDateStart: new Date('2020-11-01T04:00:00.000Z'),
              spanDateEnd: new Date('2021-02-01T05:00:00.000Z'),
            }),
          ]),
        );
      });
    });
  });
});
