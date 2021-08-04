import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { MemberMeasure } from './member-measure';

export enum SourceNames {
  commons = 'commons',
  able = 'able',
  qm_service = 'qm_service',
  elation = 'elation',
}

/* tslint:disable:member-ordering */
export class MemberMeasureSource extends BaseModel {
  static tableName = 'member_measure_source';

  id: number;
  name: string;

  static get relationMappings(): RelationMappings {
    return {
      memberMeasure: {
        relation: Model.HasManyRelation,
        modelClass: MemberMeasure,
        join: {
          from: 'member_measure_source.id',
          to: 'member_measure.sourceId',
        },
      },
    };
  }

  static async getByName(name: string, txn: Transaction) {
    return this.query(txn).whereNull('deletedAt').andWhere('name', name).first();
  }
}
/* tslint:enable:member-ordering */
