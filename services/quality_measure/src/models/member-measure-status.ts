import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { ElationMap } from './elation-map';
import { MemberMeasure } from './member-measure';

/* tslint:disable:member-ordering */
export class MemberMeasureStatus extends BaseModel {
  static tableName = 'member_measure_status';

  id: number;
  name: string;

  static get relationMappings(): RelationMappings {
    return {
      memberMeasure: {
        relation: Model.HasManyRelation,
        modelClass: MemberMeasure,
        join: {
          from: 'member_measure_status.id',
          to: 'member_measure.statusId',
        },
      },

      elationMap: {
        relation: Model.HasManyRelation,
        modelClass: ElationMap,
        join: {
          from: 'member_measure_status.id',
          to: 'elation_map.statusId',
        },
      },
    };
  }

  static async getAll(txn: Transaction) {
    return this.query(txn).whereNull('deletedAt');
  }

  static async getIdsByNames(statusNames: string[], txn: Transaction) {
    const statuses = await this.query(txn)
      .select('id')
      .whereIn('name', statusNames)
      .whereNull('deletedAt');

    return Promise.resolve(statuses.map((status) => status.id));
  }

  static async getAllIds(txn: Transaction) {
    const statuses = await this.query(txn).select('id').whereNull('deletedAt');

    return Promise.resolve(statuses.map((status) => status.id));
  }
}
/* tslint:enable:member-ordering */
