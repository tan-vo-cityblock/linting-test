import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';

export class Partner extends BaseModel {
  static tableName = 'partner';

  id!: number;
  createdAt!: Date;
  updatedAt!: Date;
  deletedAt!: Date;
  name!: string;
  deletedReason: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.HasManyRelation,
        modelClass: Member,
        join: {
          from: 'partner.id',
          to: 'member.partnerId',
        },
      },
    };
  }

  static async getByName(name: string, txn: Transaction) {
    return this.query(txn)
      .where({ name })
      .first();
  }
}
/* tslint:enable:member-ordering */
