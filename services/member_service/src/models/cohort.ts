import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';
import { Partner } from './partner';

export class Cohort extends BaseModel {
  static tableName = 'cohort';

  id!: number;
  name!: string;
  goLiveDate!: string | null;
  partnerId!: number;
  createdAt!: Date;
  updatedAt!: Date;
  deletedAt!: Date;
  deletedReason: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.HasManyRelation,
        modelClass: Member,
        join: {
          from: 'cohort.id',
          to: 'member.cohortId',
        },
      },
      partner: {
        relation: Model.BelongsToOneRelation,
        modelClass: Partner,
        join: {
          from: 'cohort.partnerId',
          to: 'partner.id',
        },
      },
    };
  }

  static async getById(id: number, txn: Transaction): Promise<Cohort> {
    return !!id ? this.query(txn).where({ id }).first() : null;
  }

  static async getByName(name: string, txn: Transaction): Promise<Cohort> {
    return !!name ? this.query(txn).where({ name }).first() : null;
  }

}
/* tslint:enable:member-ordering */
