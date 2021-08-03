import { isEmpty, isNil, omitBy } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';

export interface IAddress {
  id?: string;
  addressType?: string;
  street1?: string;
  street2?: string;
  county?: string;
  city?: string;
  state?: string;
  zip?: string;
  spanDateStart?: Date;
  spanDateEnd?: Date;
}

export interface IAddressQueryAttributes {
  addressType?: string;
  street1?: string;
  street2?: string;
  county?: string;
  city?: string;
  state?: string;
  zip?: string;
}

export class Address extends BaseModel {
  static tableName = 'address';

  id!: string;
  memberId!: string;
  addressType!: string;
  street1!: string;
  street2!: string;
  county!: string;
  city!: string;
  state!: string;
  zip!: string;
  spanDateStart!: Date;
  spanDateEnd!: Date;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'address.memberId',
          to: 'member.id',
        },
      },
    };
  }

  static async create(memberId: string, addresses: IAddress[], txn: Transaction) {
    const memberAddresses = addresses && addresses
      .map((address) => omitBy(address, isNil))
      .filter((address) => !isEmpty(address))
      .map((address) => {
        return { ...address, memberId };
      });

    if (isEmpty(memberAddresses)) {
      return null;
    }

    return this.query(txn).insertAndFetch(memberAddresses);
  }

  static async createAllNew(memberId: string, addresses: IAddress[], txn: Transaction): Promise<Address[]> {
    return Promise.all(
      addresses.map(async (address: IAddress) => this.createIfNotExists(memberId, address, txn))
    );
  }

  static async createIfNotExists(memberId: string, address: IAddress, txn: Transaction) {
    const addressQueryFilter: IAddressQueryAttributes = omitBy({
      addressType: address.addressType,
      street1: address.street1,
      street2: address.street2,
      county: address.county,
      city: address.city,
      state: address.state,
      zip: address.zip,
    }, isNil);

    const existingAddresses = await this.getAllByAttributes(addressQueryFilter, txn);
    if (!isEmpty(existingAddresses)) {
      return null;
    } else {
      return this.query(txn).insertAndFetch({ ...address, memberId });
    }
  }

  static async get(id: string, txn: Transaction) {
    return this.query(txn).findById(id);
  }

  static async getAllByAttributes(addressAttributes: IAddressQueryAttributes, txn: Transaction) {
    return this.query(txn).where({ ...addressAttributes, deletedAt: null });
  }

  static async getAllByMemberId(memberId: string, txn: Transaction): Promise<IAddress[]> {
    const addresses = await this.query(txn).where({ memberId, deletedAt: null });
    return addresses.map((address: Address) => {
      return {
        id: address.id,
        addressType: address.addressType,
        street1: address.street1,
        street2: address.street2,
        county: address.county,
        city: address.city,
        state: address.state,
        zip: address.zip,
        spanDateStart: address.spanDateStart,
        spanDateEnd: address.spanDateEnd
      } as IAddress;
    })
  }

  static async delete(
    id: string,
    deletedReason: string | undefined,
    deletedBy: string,
    txn: Transaction
  ) {
    const address = await this.get(id, txn);
    if (!address) {
      throw new Error(`attempting to delete address that does not exist [id: ${id}]`);
    } else if (address.deletedAt) {
      return address;
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
