import { every, isEmpty, isNil, omitBy, zipObject } from 'lodash';
import { Model, QueryBuilder, RelationMappings, Transaction } from 'objection';
import { Address, IAddress } from './address';
import BaseModel from './base-model';
import { Email, IEmail } from './email';
import { IMemberFilter, Member } from './member';
import { IPhone, Phone } from './phone';

export const ATTRIBUTION_UPDATED_BY: string = 'MEMBER_ATTRIBUTION';

export enum Sex {
  Female = 'female',
  Male = 'male',
}

export enum MaritalStatus {
  Divorced = 'divorced',
  DomesticPartner = 'domesticPartner',
  LegallySeparated = 'legallySeparated',
  NeverMarried = 'neverMarried',
  Unknown = 'unknown',
  Unmarried = 'unmarried',
  Widowed = 'widowed',
}

export interface IUpdateMemberDemographics {
  dateOfBirth?: string;
  dateOfDemise?: string;
  ethnicity?: string;
  firstName?: string;
  gender?: string;
  lastName?: string;
  maritalStatus?: string;
  middleName?: string;
  sex?: string;
  updatedBy: string;
}

export interface IMemberDemographics {
  firstName?: string;
  middleName?: string;
  lastName?: string;
  dateOfBirth?: string;
  dateOfDemise?: string;
  isMarkedDeceased?: boolean;
  sex?: string;
  gender?: string;
  ethnicity?: string;
  race?: string;
  language?: string;
  maritalStatus?: string;
  updatedBy?: string;
  ssn?: string;
  ssnLastFour?: string;
  shouldUpdateAddress?: boolean;
  addresses?: IAddress[];
  phones?: IPhone[];
  emails?: IEmail[];
}

export function constructMemberDemographics(memberDemographics): IMemberDemographics {
  if (!memberDemographics) {
    return {
      firstName: null,
      middleName: null,
      lastName: null,
      dateOfBirth: null,
      dateOfDemise: null,
      sex: null,
      gender: null,
      ethnicity: null,
      maritalStatus: null,
      updatedBy: null,
      ssnLastFour: null,
      addresses: [],
      phones: [],
      emails: [],
      isMarkedDeceased: false,
    };
  }

  const phones: IPhone[] = processPhones(memberDemographics.phones);
  const emails: IEmail[] = processEmails(memberDemographics.emails);
  const addresses: IAddress[] = processAddresses(memberDemographics.addresses);

  return {
    ...memberDemographics,
    phones,
    emails,
    addresses,
  };
}

export function processPhones(phones): IPhone[] {
  const names = ['id', 'phone', 'phoneType'];
  return processSqlArray<IPhone>(phones, names);
}

export function processEmails(emails): IEmail[] {
  const names = ['id', 'email'];
  return processSqlArray<IEmail>(emails, names);
}

export function processAddresses(addresses): IAddress[] {
  const names = [
    'id',
    'addressType',
    'street1',
    'street2',
    'county',
    'city',
    'state',
    'zip',
    'spanDateStart',
    'spanDateEnd',
  ];
  return processSqlArray<IAddress>(addresses, names);
}

/**
 * This function first verifies that the (nested) array provided is not null and that every
 * sub-array has the same length as the keys we wish to map from columnNames.
 *
 * Once the check is verified, we iterate through each array in the string[][] and convert it into
 * an object by zipping each array we iterate with it's respective key.
 *
 * @param nestedArray: An array that is received from the SQL groupBy subquery.
 * @param columnNames: The name of the columns / jsonKey that we wish to map the database entries to.
 */
function processSqlArray<T>(nestedArray: string[][], columnNames: string[]): T[] {
  if (nestedArray && every(nestedArray, (element) => element.length === columnNames.length)) {
    return nestedArray.map((element) => zipObject(columnNames, element));
  } else if (nestedArray) {
    throw new Error(
      `Column mismatch between database: ${nestedArray} and jsonSchema: ${columnNames} for Address.`,
    );
  }
  return [];
}

export class MemberDemographics extends BaseModel {
  static tableName = 'member_demographics';

  memberId!: string;
  firstName!: string;
  middleName!: string;
  lastName!: string;
  dateOfBirth!: string;
  isMarkedDeceased!: boolean;
  dateOfDemise!: string;
  sex!: string;
  gender!: string;
  ethnicity!: string;
  race!: string;
  language!: string;
  maritalStatus!: string;
  ssn!: string;
  ssnLastFour!: string;
  spanDateStart!: Date;
  spanDateEnd!: Date;
  deletedReason!: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'member_demographics.memberId',
          to: 'member.id',
        },
      },
    };
  }

  static modifiers = {
    defaultSelects(query) {
      query.select(
        'firstName',
        'middleName',
        'lastName',
        'dateOfBirth',
        'dateOfDemise',
        'isMarkedDeceased',
        'sex',
        'gender',
        'ethnicity',
        'race',
        'language',
        'maritalStatus',
        'ssnLastFour',
      );
    },
  };

  static demographicsSubQuery(
    filter: IMemberFilter,
    txn: Transaction,
  ): QueryBuilder<MemberDemographics> {
    return this.query(txn)
      .select('memberId')
      .modify('defaultSelects')
      .modify((builder) => {
        if (filter.memberIds) {
          builder.whereIn('memberId', filter.memberIds);
        }
      })
      .whereNull('deletedAt');
  }

  static phoneSubQuery(filter: IMemberFilter, txn: Transaction): QueryBuilder<Phone> {
    return Phone.query(txn)
      .select(
        'memberId',
        txn.raw('array_agg(array["id" ::TEXT, "phone", "phoneType"::TEXT]) as phones'),
      )
      .whereNull('deletedAt')
      .modify((builder) => {
        if (filter.memberIds) {
          builder.whereIn('memberId', filter.memberIds);
        }
      })
      .groupBy('memberId');
  }

  static emailSubQuery(filter: IMemberFilter, txn: Transaction): QueryBuilder<Email> {
    return Email.query(txn)
      .select('memberId', txn.raw('array_agg(array["id" ::TEXT, "email"]) as emails'))
      .whereNull('deletedAt')
      .modify((builder) => {
        if (filter.memberIds) {
          builder.whereIn('memberId', filter.memberIds);
        }
      })
      .groupBy('memberId');
  }

  static addressSubQuery(filter: IMemberFilter, txn: Transaction): QueryBuilder<Address> {
    return Address.query(txn)
      .select(
        'memberId',
        txn.raw(
          'array_agg(array["id"::TEXT, "addressType", "street1", "street2", "county", "city", "state", "zip", "spanDateStart"::TEXT, "spanDateEnd"::TEXT]) as addresses',
        ),
      )
      .whereNull('deletedAt')
      .modify((builder) => {
        if (filter.memberIds) {
          builder.whereIn('memberId', filter.memberIds);
        }
      })
      .groupBy('memberId');
  }

  static async getByMemberId(memberId: string, txn: Transaction): Promise<IMemberDemographics> {
    const _phoneSubQuery_ = MemberDemographics.phoneSubQuery(
      { memberIds: [memberId] } as IMemberFilter,
      txn,
    );
    const _emailSubQuery_ = MemberDemographics.emailSubQuery(
      { memberIds: [memberId] } as IMemberFilter,
      txn,
    );
    const _addressSubQuery_ = MemberDemographics.addressSubQuery(
      { memberIds: [memberId] } as IMemberFilter,
      txn,
    );

    const results = this.query(txn)
      .modify('defaultSelects')
      .select('phone.phones', 'email.emails', 'address.addresses')
      .leftOuterJoin(
        txn.raw(`(${_phoneSubQuery_}) AS phone`),
        'member_demographics.memberId',
        'phone.memberId',
      )
      .leftOuterJoin(
        txn.raw(`(${_emailSubQuery_}) as email`),
        'member_demographics.memberId',
        'email.memberId',
      )
      .leftOuterJoin(
        txn.raw(`(${_addressSubQuery_}) as address`),
        'member_demographics.memberId',
        'address.memberId',
      )
      .whereNull('member_demographics.deletedAt')
      .andWhere('member_demographics.memberId', memberId);

    const memberQueryResult = await results.first();
    return constructMemberDemographics(memberQueryResult);
  }

  static async create(memberId: string, memberDemographics: IMemberDemographics, txn: Transaction) {
    const partialDemographics = omitBy(memberDemographics, isNil);
    if (isEmpty(partialDemographics)) {
      return null;
    }

    const fullDemographics = {
      ...partialDemographics,
      memberId,
      phones: undefined,
      addresses: undefined,
      emails: undefined,
    };
    return this.query(txn).insertAndFetch(fullDemographics);
  }

  static async getById(id: string, txn: Transaction) {
    return this.query(txn).findById(id);
  }

  static async updateById(
    id: string,
    updatedDemographics: IUpdateMemberDemographics,
    txn: Transaction,
  ) {
    return this.query(txn).patchAndFetchById(id, updatedDemographics);
  }

  static async getLatestByMemberId(memberId: string, txn: Transaction) {
    return this.query(txn).findOne({ memberId, deletedAt: null });
  }

  static async getAllByMemberId(memberId: string, txn: Transaction) {
    return this.query(txn).where({ memberId });
  }

  static async updateAndSaveHistory(
    memberId: string,
    updatedDemographics: IUpdateMemberDemographics,
    txn: Transaction,
  ) {
    const { updatedBy } = updatedDemographics || null;
    const currentMemberDemographics = await this.getLatestByMemberId(memberId, txn);

    await this.query(txn)
      .findOne({ memberId, deletedAt: null })
      .patch({ deletedAt: new Date(), deletedBy: updatedBy });

    const updateMetaFields = { id: undefined, createdAt: new Date(), updatedBy };
    // TODO: should we compact the updated fields below?
    const updatedMemberDemographics = Object.assign(
      { memberId },
      currentMemberDemographics,
      updatedDemographics,
      updateMetaFields,
    );

    return this.query(txn).insertAndFetch(updatedMemberDemographics);
  }
}
