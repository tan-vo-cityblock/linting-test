import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Measure } from './measure';
import { MemberMeasureStatus } from './member-measure-status';

/* tslint:disable:member-ordering */
export class ElationMap extends BaseModel {
  static tableName = 'elation_map';

  id: number;
  codeType: string;
  code: string;
  measureId: number;
  statusId: number;
  measure: Measure;
  memberMeasureStatus: MemberMeasureStatus;

  static get relationMappings(): RelationMappings {
    return {
      measure: {
        relation: Model.BelongsToOneRelation,
        modelClass: Measure,
        join: {
          from: 'elation_map.measureId',
          to: 'measure.id',
        },
      },

      memberMeasureStatus: {
        relation: Model.BelongsToOneRelation,
        modelClass: MemberMeasureStatus,
        join: {
          from: 'elation_map.statusId',
          to: 'member_measure_status.id',
        },
      },
    };
  }

  static async getAllEager(txn: Transaction) {
    return this.query(txn)
      .whereNull('deletedAt')
      .withGraphFetched('[measure, memberMeasureStatus]');
  }
}
