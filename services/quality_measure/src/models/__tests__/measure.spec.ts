import { last } from 'lodash';
import { transaction } from 'objection';
import { setupDb } from '../../util/test-utils';
import { IMeasurePerformanceRange, Measure } from '../measure';

describe('Measure model', () => {
  let testDb = null as any;
  let txn = null as any;
  let measures: Measure[];

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => {
    testDb.destroy();
  });

  beforeEach(async () => {
    txn = await transaction.start(Measure.knex());
    measures = await Measure.getAll(txn);
  });

  afterEach(async () => {
    await txn.rollback();
  });

  describe('getAll and getAllIds', () => {
    it('fetches all measures', async () => {
      expect(measures).toHaveLength(15);
      expect(last(measures).code).toBe('ABLE FVO');
      expect(last(measures).rateId).toBe(0);
    });

    it('fetches all measures IDs', async () => {
      const measureIds = await Measure.getAllIds(txn);
      expect(measureIds).toHaveLength(15);
      expect(last(measureIds)).toBe(17);
    });
  });

  describe('getPerformanceRangeForDate', () => {
    it('returns the correct performance date range for a given date', async () => {
      const testSetOn = new Date(2021, 3 - 1, 1);

      const year = 2021;
      const prevYear = year - 1;
      const nextYear = year + 1;
      const day = 1;

      const fluMonth = 7 - 1; // month is 0 indexed
      const fluRangeStart = new Date(Date.UTC(prevYear, fluMonth, day));
      const fluRangeEnd = new Date(Date.UTC(year, fluMonth, day));
      const fluPerformanceRange: IMeasurePerformanceRange = {
        setOnYearStart: fluRangeStart.toISOString(),
        setOnYearEnd: fluRangeEnd.toISOString(),
      };

      const nonFluMonth = 1 - 1; // month is 0 indexed
      const nonFluRangeStart = new Date(Date.UTC(year, nonFluMonth, day));
      const nonFluRangeEnd = new Date(Date.UTC(nextYear, nonFluMonth, day));
      const nonFluPerformanceRange: IMeasurePerformanceRange = {
        setOnYearStart: nonFluRangeStart.toISOString(),
        setOnYearEnd: nonFluRangeEnd.toISOString(),
      };

      const results = measures.map((measure) => {
        const code = measure.code;
        if (code.includes('FVA') || code.includes('FVO')) {
          return [measure, fluPerformanceRange];
        } else {
          return [measure, nonFluPerformanceRange];
        }
      });

      const testData = measures.map((measure) => {
        return [measure, measure.getPerformanceRangeForDate(testSetOn)];
      });

      expect(testData).toEqual(results);
    });
  });
});
