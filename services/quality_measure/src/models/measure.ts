import { Model, RelationMappings, Transaction } from 'objection';
import { IPostgresInterval } from 'postgres-interval';
import { isDateInRangeExclusive } from '../util/helper';
import BaseModel from './base-model';
import { ElationMap } from './elation-map';
import { MarketMeasure } from './market-measure';
import { MeasureCategory } from './measure-category';
import { MemberMeasure } from './member-measure';

export enum MeasureType {
  process = 'process',
  outcome = 'outcome',
}

export enum MeasureTrigger {
  annual = 'annual',
  event = 'event',
}
export interface IMeasurePerformanceRange {
  setOnYearStart: string;
  setOnYearEnd: string;
}

/* tslint:disable:max-classes-per-file */
class PerformanceRangeError extends Error {
  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, PerformanceRangeError.prototype);
  }
}

/* tslint:disable:member-ordering */
export class Measure extends BaseModel {
  static tableName = 'measure';

  id: number;
  code: string;
  rateId: number; // Defaults to 0 if null in able data. rateId + code should be unique.
  name: string;
  displayName: string;
  categoryId: string | null;
  type: MeasureType;
  trigger: MeasureTrigger;
  triggerCloseBy: IPostgresInterval | null;

  static get relationMappings(): RelationMappings {
    return {
      measureCategory: {
        relation: Model.BelongsToOneRelation,
        modelClass: MeasureCategory,
        join: {
          from: 'measure.categoryId',
          to: 'measure_category.id',
        },
      },
      memberMeasure: {
        relation: Model.HasManyRelation,
        modelClass: MemberMeasure,
        join: {
          from: 'measure.id',
          to: 'memberMeasure.measureId',
        },
      },
      marketMeasure: {
        relation: Model.HasManyRelation,
        modelClass: MarketMeasure,
        join: {
          from: 'measure.id',
          to: 'market_measure.measureId',
        },
      },
      elationMap: {
        relation: Model.HasManyRelation,
        modelClass: ElationMap,
        join: {
          from: 'measure.id',
          to: 'elation_map.measureId',
        },
      },
    };
  }

  static async getAll(txn: Transaction) {
    return this.query(txn).whereNull('deletedAt');
  }

  static async getAllIds(txn: Transaction) {
    const measures = await this.query(txn).select('id').whereNull('deletedAt');
    return Promise.resolve(measures.map((measure) => measure.id));
  }

  private static getPerformanceRangeForFlu(
    measureSetOn: Date,
    code: string,
  ): IMeasurePerformanceRange {
    if (!code.includes('FVA') && !code.includes('FVO')) {
      throw new PerformanceRangeError(
        `Measure code: "${code}" does not contain flu codes: "[FVA, FVO]"`,
      );
    }

    const measureYear = measureSetOn.getUTCFullYear();
    const prevYear = measureYear - 1;
    const nextYear = measureYear + 1;

    const month = 7 - 1; // month is 0 indexed
    const day = 1;

    const prevSetOnYearStart = new Date(Date.UTC(prevYear, month, day));
    const prevSetOnYearEnd = new Date(Date.UTC(measureYear, month, day));

    const isSetOnInPrevRange = isDateInRangeExclusive(
      prevSetOnYearStart,
      measureSetOn,
      prevSetOnYearEnd,
    );

    const nextSetOnYearStart = new Date(Date.UTC(measureYear, month, day));
    const nextSetOnYearEnd = new Date(Date.UTC(nextYear, month, day));

    if (isSetOnInPrevRange) {
      return {
        setOnYearStart: prevSetOnYearStart.toISOString(),
        setOnYearEnd: prevSetOnYearEnd.toISOString(),
      };
    } else {
      return {
        setOnYearStart: nextSetOnYearStart.toISOString(),
        setOnYearEnd: nextSetOnYearEnd.toISOString(),
      };
    }
  }

  getPerformanceRangeForDate(measureSetOn: Date): IMeasurePerformanceRange {
    try {
      return Measure.getPerformanceRangeForFlu(measureSetOn, this.code);
    } catch (err) {
      if (err instanceof PerformanceRangeError) {
        const measureYear = measureSetOn.getUTCFullYear();
        const nextYear = measureYear + 1;

        const month = 1 - 1; // month is 0 indexed
        const day = 1;

        const setOnYearStart = new Date(Date.UTC(measureYear, month, day));
        const setOnYearEnd = new Date(Date.UTC(nextYear, month, day));

        return {
          setOnYearStart: setOnYearStart.toISOString(),
          setOnYearEnd: setOnYearEnd.toISOString(),
        };
      } else {
        console.error('error in the model Measure', err);
      }
    }
  }
}
/* tslint:enable:member-ordering */
