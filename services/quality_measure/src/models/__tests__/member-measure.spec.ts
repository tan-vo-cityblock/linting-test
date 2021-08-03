import { omit } from 'lodash';
import { transaction } from 'objection';
import { v4 as uuid } from 'uuid';
import { setupDb } from '../../util/test-utils';
import { MemberMeasure } from '../member-measure';

const currDate = new Date();
const currTimeMilliEpoch = currDate.getTime();
const currYear = currDate.getUTCFullYear();

const sourceNameAble = 'able';
const sourceNameCommons = 'commons';
const nyMarketSlug = 'new-york-city';
const memberIdA = uuid();
const memberIdB = uuid();
const memberIdC = uuid();
const userId = uuid();

// TODO: Add helper fxns to pull ids in below consts by querying db.
const statusOpen = {
  id: 2,
  name: 'open',
};

const statusProvClosed = {
  id: 3,
  name: 'provClosed',
};

const statusClosed = {
  id: 4,
  name: 'closed',
};

const measureBMI = {
  id: 1,
  code: 'HEDIS ABA',
};
const measureWell1 = {
  id: 2,
  code: 'ABLE AWV1',
};
const measureBreastCan = {
  id: 6,
  code: 'HEDIS BCS',
};

const measureEyeExam = {
  id: 11,
  code: 'HEDIS CDC6',
};

const measureHighBP = {
  id: 13,
  code: 'HEDIS CBP',
};

const measureFluFVO = {
  id: 17,
  code: 'ABLE FVO',
};

const measureStatusReq = {
  code: measureBMI.code,
  rateId: null,
  status: statusOpen.name,
  setOn: currDate.toISOString(),
};

function compareId(a: any, b: any) {
  return a.id - b.id;
}

function omitEagerMemberMeasure(memberMeasure: MemberMeasure) {
  return omit(memberMeasure, ['memberMeasureStatus', 'measure', 'market']);
}

describe('MemberMeasure model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(MemberMeasure.knex());
  });

  afterEach(async () => {
    await txn.rollback();
  });

  describe('insertOrPatchMemberMeasures', () => {
    describe('when new member measure is an outcome type measure', () => {
      it('throws an error if the status is provClosed', async () => {
        const throwForInvalidStatus = async () => {
          await MemberMeasure.insertOrPatchMemberMeasures(
            memberIdA,
            sourceNameAble,
            [
              {
                ...measureStatusReq,
                code: measureHighBP.code,
                status: statusProvClosed.name,
              },
            ],
            txn,
          );
        };
        const expectedErrorMessage = `Invalid measure status: '${statusProvClosed.name}' for outcome measure: '${measureHighBP.code}'`;
        await expect(throwForInvalidStatus()).rejects.toThrowError(expectedErrorMessage);
      });
    });
    describe('when no previous member measure exists in db', () => {
      it('inserts new member measure from Able', async () => {
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(newMemberMeasure).toHaveLength(1);
        expect(newMemberMeasure).toEqual(memberMeasureTableRowsEager);
      });

      it('does not insert new member measure for first time if source is not able', async () => {
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameCommons,
          [
            {
              ...measureStatusReq,
              status: statusProvClosed.name,
              userId,
            },
          ],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(newMemberMeasure).toHaveLength(1);
        expect(newMemberMeasure).toEqual([undefined]);
        expect(memberMeasureTableRowsEager).toHaveLength(0);
      });
    });
    describe('when prev member measure exists, and new member measure and prev member measure have different setOn.year', () => {
      it('inserts new member measure even if statuses are the same', async () => {
        const prevMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const futureDate = new Date(currTimeMilliEpoch);
        futureDate.setUTCFullYear(currYear + 1);
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              setOn: futureDate.toISOString(),
            },
          ],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(prevMemberMeasure).toHaveLength(1);
        expect(prevMemberMeasure.concat(newMemberMeasure).sort(compareId)).toEqual(
          memberMeasureTableRowsEager.sort(compareId),
        );
      });
    });
    describe('when prev member measure exists, and new member measure and prev member measure share the same setOn.year', () => {
      it('updates prev member measure lastConfirmed if prev member measure has same status as new member measure', async () => {
        const prevMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const futureDate = new Date(currTimeMilliEpoch + 1);
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [{ ...measureStatusReq, setOn: futureDate.toISOString() }],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(newMemberMeasure).toHaveLength(1);
        expect(newMemberMeasure).toEqual(memberMeasureTableRowsEager);
        expect(newMemberMeasure[0].lastConfirmed.getTime()).toBeGreaterThan(
          prevMemberMeasure[0].lastConfirmed.getTime(),
        );
      });
      it('inserts new member measure if prev member measure exists with different status and older setOn date as new measure', async () => {
        const prevMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const futureDate = new Date(currTimeMilliEpoch + 1);
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              status: statusClosed.name,
              setOn: futureDate.toISOString(),
            },
          ],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(prevMemberMeasure).toHaveLength(1);
        expect(prevMemberMeasure.concat(newMemberMeasure).sort(compareId)).toEqual(
          memberMeasureTableRowsEager.sort(compareId),
        );
      });

      it('inserts new member measure from Commons w/ userId if prev member measure exists with different status and older setOn date as new measure', async () => {
        const prevMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const futureDate = new Date(currTimeMilliEpoch + 1);
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameCommons,
          [
            {
              ...measureStatusReq,
              status: statusProvClosed.name,
              setOn: futureDate.toISOString(),
              userId,
            },
          ],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(newMemberMeasure).toHaveLength(1);
        expect(prevMemberMeasure.concat(newMemberMeasure).sort(compareId)).toEqual(
          memberMeasureTableRowsEager.sort(compareId),
        );
      });

      it('does nothing if new member measure has different status and older setOn date than prev member measure', async () => {
        const prevMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        );

        const pastDate = new Date(currTimeMilliEpoch - 1);
        const newMemberMeasure = await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              status: 'closed',
              setOn: pastDate.toISOString(),
            },
          ],
          txn,
        );

        const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
          .where('memberId', memberIdA)
          .withGraphFetched('[memberMeasureStatus, measure]');

        expect(prevMemberMeasure).toHaveLength(1);
        expect(prevMemberMeasure).toEqual(memberMeasureTableRowsEager);
        expect(newMemberMeasure).toEqual(prevMemberMeasure);
      });
    });
  });
  describe('getMemberMeasures', () => {
    it('returns unique member measures using most recent setOn timestamp', async () => {
      await MemberMeasure.insertOrPatchMemberMeasures(
        memberIdA,
        sourceNameAble,
        [measureStatusReq],
        txn,
      );

      const resultInsert2 = await MemberMeasure.insertOrPatchMemberMeasures(
        memberIdA,
        sourceNameAble,
        [
          {
            ...measureStatusReq,
            status: statusOpen.name,
            setOn: new Date().toISOString(),
          },
        ],
        txn,
      );

      const resultInsert3 = await MemberMeasure.insertOrPatchMemberMeasures(
        memberIdA,
        sourceNameAble,
        [
          {
            ...measureStatusReq,
            code: measureWell1.code,
            status: statusOpen.name,
            setOn: new Date().toISOString(),
          },
        ],
        txn,
      );

      const resultGet = await MemberMeasure.getMemberMeasures(memberIdA, nyMarketSlug, txn);

      const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
        .where('memberId', memberIdA)
        .withGraphFetched('[memberMeasureStatus, measure]');

      expect(resultGet).toHaveLength(2);
      expect(resultGet.sort(compareId)).toEqual(memberMeasureTableRowsEager.sort(compareId));
      expect(resultGet.sort(compareId)).toEqual(
        resultInsert2.concat(resultInsert3).sort(compareId),
      );
    });

    it('does not return member measures from a different marketSlug', async () => {
      const nyMeasureInsert = await MemberMeasure.insertOrPatchMemberMeasures(
        memberIdA,
        sourceNameAble,
        [{ ...measureStatusReq, code: measureHighBP.code, status: statusOpen.name }],
        txn,
      );

      const resultGet = await MemberMeasure.getMemberMeasures(memberIdA, nyMarketSlug, txn);

      const memberMeasureTableRowsEager = await MemberMeasure.query(txn)
        .where('memberId', memberIdA)
        .withGraphFetched('[memberMeasureStatus, measure]');

      expect(resultGet).toHaveLength(0);
      expect(nyMeasureInsert).toEqual(memberMeasureTableRowsEager);
    });
  });

  describe('getMemberIdsFromQuery', () => {
    it('returns unique memberIds that match query for measureIds, statusIds, and setOn year using most recent setOn timestamp', async () => {
      // Insert open bmi for member A
      (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [measureStatusReq],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert closed bmi for member A
      const insertMemberA1B = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [{ ...measureStatusReq, status: statusClosed.name, setOn: new Date().toISOString() }],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert open well1 for member A
      const insertMemberA2A = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [{ ...measureStatusReq, code: measureWell1.code, setOn: new Date().toISOString() }],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert open eye exam for member A
      const insertMemberA3A = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              code: measureEyeExam.code,
              status: statusOpen.name,
              setOn: new Date().toISOString(),
            },
          ],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert open well 1 for member B
      const insertMemberB1A = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdB,
          sourceNameAble,
          [{ ...measureStatusReq, code: measureWell1.code, setOn: new Date().toISOString() }],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert closed breast can for member B
      const insertMemberB1B = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdB,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              code: measureBreastCan.code,
              status: statusClosed.name,
              setOn: new Date().toISOString(),
            },
          ],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert open eye exam for member B
      const insertMemberB2A = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdB,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              code: measureEyeExam.code,
              status: statusOpen.name,
              setOn: new Date().toISOString(),
            },
          ],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // Insert open BMI, eye exam, breast cancer, wellness for member C
      const insertMemberC1A = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdC,
          sourceNameAble,
          [
            measureStatusReq,
            { ...measureStatusReq, code: measureEyeExam.code },
            { ...measureStatusReq, code: measureBreastCan.code },
            { ...measureStatusReq, code: measureWell1.code },
          ],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      // query for open bmi
      const queryResultA = await MemberMeasure.getMemberMeasuresFromQuery(
        [measureBMI.id],
        [statusOpen.id],
        [memberIdA, memberIdB],
        txn,
      );
      expect(queryResultA).toHaveLength(0);

      // query for closed bmi
      const queryResultB = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [measureBMI.id],
          [statusClosed.id],
          [memberIdA, memberIdB],
          txn,
        )
      ).map(omitEagerMemberMeasure);
      expect(queryResultB).toHaveLength(1);
      expect(queryResultB).toEqual(insertMemberA1B);

      // query for open well 1
      const queryResultC = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [measureWell1.id],
          [statusOpen.id],
          [memberIdA, memberIdB],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      expect(queryResultC).toHaveLength(2);
      expect(queryResultC.sort(compareId)).toEqual(
        insertMemberA2A.concat(insertMemberB1A).sort(compareId),
      );

      // query for open or closed well1, and open or closed breast can
      const queryResultD = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [measureWell1.id, measureBreastCan.id],
          [statusOpen.id, statusClosed.id],
          [memberIdA, memberIdB],
          txn,
        )
      ).map(omitEagerMemberMeasure);
      expect(queryResultD).toHaveLength(2);
      expect(queryResultD.sort(compareId)).toEqual(
        insertMemberB1A.concat(insertMemberB1B).sort(compareId),
      );

      // query for open well1 and open eye exam
      const queryResultE = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [measureWell1.id, measureEyeExam.id],
          [statusOpen.id],
          [memberIdA, memberIdB],
          txn,
        )
      ).map(omitEagerMemberMeasure);
      expect(queryResultE).toHaveLength(4);
      expect(queryResultE.sort(compareId)).toEqual(
        insertMemberA2A.concat(insertMemberA3A, insertMemberB1A, insertMemberB2A).sort(compareId),
      );

      // query for open or closed well1, and open or closed breast can, and open or closed bmi
      const queryResultF = await MemberMeasure.getMemberMeasuresFromQuery(
        [measureWell1.id, measureBreastCan.id, measureBMI.id],
        [statusOpen.id, statusClosed.id],
        [memberIdA, memberIdB],
        txn,
      );
      expect(queryResultF).toHaveLength(0);

      // query for open status only
      const queryResultG = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [],
          [statusOpen.id],
          [memberIdA, memberIdB, memberIdC],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      expect(queryResultG).toHaveLength(8);
      expect(queryResultG.sort(compareId)).toEqual(
        insertMemberA2A
          .concat(insertMemberA3A, insertMemberB1A, insertMemberB2A, insertMemberC1A)
          .sort(compareId),
      );
    });
  });

  describe('For Flu, getMemberMeasures and getMemberIdsFromQuery', () => {
    it('returns correct data given performance date range overlapping calendar year', async () => {
      const setOnA = new Date(Date.UTC(currYear - 1, 6 - 1, 30)).toISOString();
      await MemberMeasure.insertOrPatchMemberMeasures(
        memberIdA,
        sourceNameAble,
        [
          {
            ...measureStatusReq,
            code: measureFluFVO.code,
            status: statusClosed.name,
            setOn: setOnA,
          },
        ],
        txn,
      );

      const setOnB = new Date(Date.UTC(currYear - 1, 7 - 1, 1)).toISOString();
      const fluInsertB = (
        await MemberMeasure.insertOrPatchMemberMeasures(
          memberIdA,
          sourceNameAble,
          [
            {
              ...measureStatusReq,
              code: measureFluFVO.code,
              status: statusOpen.name,
              setOn: setOnB,
            },
          ],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      const resultGetMemberMeasures = (
        await MemberMeasure.getMemberMeasures(memberIdA, nyMarketSlug, txn)
      ).map(omitEagerMemberMeasure);

      expect(resultGetMemberMeasures).toHaveLength(1);
      expect(resultGetMemberMeasures).toEqual(fluInsertB);

      // query for flu
      const resultQueryMemberMeasure = (
        await MemberMeasure.getMemberMeasuresFromQuery(
          [measureFluFVO.id],
          [statusOpen.id, statusClosed.id],
          [memberIdA],
          txn,
        )
      ).map(omitEagerMemberMeasure);

      expect(resultQueryMemberMeasure).toHaveLength(1);
      expect(resultQueryMemberMeasure).toEqual(fluInsertB);
    });
  });
});
