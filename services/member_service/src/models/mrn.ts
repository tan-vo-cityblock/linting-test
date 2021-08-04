import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';
import { MemberInsuranceMapping } from './member-insurance';

export const ELATION: string = 'elation';
export const ACPNY: string = 'acpny';

export interface IMedicalRecordNumber {
  id: string;
  name: string;
}

export function isMrn(datasource) {
  return [ELATION, ACPNY].includes(datasource);
}

export class MedicalRecordNumber extends BaseModel {
  static tableName = 'mrn';

  id!: string;
  mrn!: string;
  name!: string;
  createdAt!: Date;
  updatedAt!: Date;
  deletedAt!: Date;
  deletedReason!: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'mrn.id',
          to: 'member.mrnId',
        },
      },
    };
  }

  static async create(mrnId: string, mrnName: string, txn: Transaction) {
    return this.query(txn).insertAndFetch({
      mrn: mrnId,
      name: mrnName,
    });
  }

  static async getWhere(
    fieldName: string,
    fieldValue: string,
    txn: Transaction,
  ): Promise<IMedicalRecordNumber> {
    return this.query(txn)
      .select('mrn.name as name', 'mrn.mrn as id')
      .innerJoin('member', 'mrn.id', 'member.mrnId')
      .modify((builder) => {
        builder.whereNull('mrn.deletedAt').andWhere(fieldName, fieldValue);
      })
      .whereNull('member.deletedAt')
      .first();
  }

  static async getByMrn(mrn: string, txn: Transaction) {
    return this.getWhere('mrn.mrn', mrn, txn);
  }

  static async getMrn(memberId: string, txn: Transaction): Promise<IMedicalRecordNumber> {
    return this.getWhere('member.id', memberId, txn);
  }

  static async getMrnMapping(
    filter: { memberId?: string; externalId?: string; carrier?: string },
    txn: Transaction,
  ): Promise<MemberInsuranceMapping[]> {
    return this.query(txn)
      .select(
        'member.id as memberId',
        'mrn.mrn as externalId',
        'mrn.name as carrier',
        MedicalRecordNumber.knex().raw('true as current'),
      )
      .innerJoin('member', 'mrn.id', 'member.mrnId')
      .modify((builder) => {
        if (filter.memberId) {
          builder.where('member.id', filter.memberId);
        }
        if (filter.carrier) {
          builder.where('mrn.name', filter.carrier);
        }
        if (filter.externalId) {
          builder.where('mrn.mrn', filter.externalId);
        }
      })

      .whereNull('mrn.deletedAt')
      .whereNull('member.deletedAt') as any;
  }

  static async updateOrCreateMrn(
    memberId: string,
    mrn: string,
    name: string,
    txn: Transaction,
  ): Promise<IMedicalRecordNumber> {
    const member: Member = await Member.query(txn)
      .where({ id: memberId })
      .whereNull('deletedAt')
      .first();

    let mrnEntry: MedicalRecordNumber;
    if (!member.mrnId) {
      mrnEntry = await this.create(mrn, name, txn);
      await Member.query(txn)
        .findById(memberId)
        .patch({ mrnId: mrnEntry.id, updatedAt: new Date() });
    } else {
      mrnEntry = await this.updateMrn(member.mrnId, name, mrn, txn);
    }

    return { id: mrnEntry.mrn, name: mrnEntry.name };
  }

  static async updateMrn(id: string, name: string, mrn: string, txn: Transaction) {
    return this.query(txn)
      .patch({ name, mrn, deletedAt: null, deletedReason: null })
      .where({ id })
      .returning('*')
      .first();
  }

  // TODO: Use this function when implementing delete controller for mrn endpoint.
  static async deleteMrnIfExists(memberId: string, deletedReason: string, txn: Transaction) {
    const memberMrns = await this.query(txn)
      .select('mrn.id as id', 'mrn.mrn as mrn', 'mrn.name as name')
      .innerJoin('member', 'mrn.id', 'member.mrnId')
      .modify((builder) => {
        builder.whereNull('mrn.deletedAt').andWhere('member.id', memberId);
      })
      .whereNull('member.deletedAt');

    if (memberMrns.length > 0) {
      const memberMrn = memberMrns[0];
      return this.query(txn)
        .patch({ updatedAt: new Date(), deletedAt: new Date(), deletedReason })
        .where({ id: memberMrn.id })
        .returning('*')
        .first();
    } else {
      return null;
    }
  }
}
/* tslint:enable:member-ordering */
