import { isEmpty, isNil, omitBy } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';

export interface IPhone {
  id?: string;
  phone: string;
  phoneType?: string;
}

export interface IPhoneQueryAttributes {
  phone?: string;
  phoneType?: string;
}

// tslint:disable: member-ordering
export class Phone extends BaseModel {
  static tableName = 'phone';

  id!: string;
  memberId!: string;
  phone!: string;
  phoneType!: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'phone.memberId',
          to: 'member.id',
        },
      },
    };
  }

  static async create(memberId: string, phones: IPhone[], txn: Transaction) {
    const memberPhones = phones && phones
      .map((phone) => omitBy(phone, isNil))
      .filter((phone) => !isEmpty(phone))
      .map((phone) => {
        return { ...phone, memberId };
      });

    if (isEmpty(memberPhones)) {
      return null;
    }

    return this.query(txn).insertAndFetch(memberPhones);
  }

  static async createAllNew(memberId: string, phones: IPhone[], txn: Transaction): Promise<Phone[]> {
    return Promise.all(
      phones.map(async (phone: IPhone) => this.createIfNotExists(memberId, phone, txn))
    );
  }

  static async createIfNotExists(memberId: string, phone: IPhone, txn: Transaction): Promise<Phone> {
    const phoneQueryFilter: IPhoneQueryAttributes = omitBy({
      phone: phone.phone,
      phoneType: phone.phoneType,
    }, isNil);

    const existingPhones = await this.getAllByAttributes(phoneQueryFilter, txn);
    if (!isEmpty(existingPhones)) {
      return null;
    } else {
      return this.query(txn).insertAndFetch({ ...phone, memberId });
    }
  }

  static async get(id: string, txn: Transaction) {
    return this.query(txn).findById(id);
  }

  static async getAllByAttributes(phoneAttributes: IPhoneQueryAttributes, txn: Transaction) {
    return this.query(txn).where({ ...phoneAttributes, deletedAt: null });
  }
  
  static async getAllByMemberId(memberId: string, txn: Transaction) {
    return this.query(txn).where({ memberId, deletedAt: null });
  }

  static async delete(
    id: string,
    deletedReason: string | undefined,
    deletedBy: string,
    txn: Transaction
  ) {
    const phone = await this.get(id, txn);
    if (!phone) {
      throw new Error(`attempting to delete phone that does not exist [id: ${id}]`);
    } else if (phone.deletedAt) {
      return phone;
    }

    const deletePatch = {
      deletedAt: new Date(),
      deletedReason: deletedReason || null,
      deletedBy
    };

    return this.query(txn)
      .where({ id, deletedAt: null })
      .patchAndFetchById(id, deletePatch);
  }

}
