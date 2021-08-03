import { isNil, omitBy } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { MemberInsurance } from './member-insurance';

export class Datasource extends BaseModel {
  static tableName = 'datasource';

  id!: number;
  name!: string;
  createdAt!: Date;
  updatedAt!: Date;
  deletedAt!: Date;
  deletedReason: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.HasManyRelation,
        modelClass: MemberInsurance,
        join: {
          from: 'datasource.id',
          to: 'member_insurance.datasourceId',
        },
      },
    };
  }

  static async getByName(datasourceName: string, txn: Transaction) {
    const datasource = await this.query(txn).where({ name: datasourceName }).first();
    if (!datasource) {
      throw new Error(`Invalid datasource name: ${datasourceName}`);
    }
    return datasource;
  }

  static async getByFields(
    carrier: string,
    lineOfBusiness: string | null,
    subLineOfBusiness: string | null,
    txn: Transaction,
  ) {
    if (!carrier) {
      throw new Error(`Field "carrier" can not be null`);
    }
    const queryFilter = omitBy({ name: carrier, lineOfBusiness, subLineOfBusiness }, isNil);
    return this.query(txn).findOne(queryFilter);
  }
}
/* tslint:enable:member-ordering */
