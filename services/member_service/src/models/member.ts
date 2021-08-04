import { filter, groupBy, head, isEmpty, isNil, omitBy, some, startsWith } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import validate from 'uuid-validate';
import { deletePatientIfElation } from '../util/elation';
import { deleteZendeskUser } from '../util/zendesk';
import { Address, IAddress } from './address';
import BaseModel from './base-model';
import { Category } from './category';
import { Cohort } from './cohort';
import { Datasource } from './datasource';
import { Email, IEmail } from './email';
import {
  IConsolidateAndUpdateDetailsInput,
  IInsurance,
  MemberInsurance,
  STATE_IDS
} from './member-insurance';
import {
  processAddresses,
  processEmails,
  processPhones,
  IMemberDemographics,
  IUpdateMemberDemographics,
  MemberDemographics,
} from './member-demographics';
import { IInsuranceDetails } from './member-insurance-details';
import { isMrn, ELATION, IMedicalRecordNumber, MedicalRecordNumber } from './mrn';
import { Partner } from './partner';
import { IPhone, Phone } from './phone';

export interface IMemberFilter {
  datasource?: string;
  externalIds?: string[];
  memberIds?: string[];
  zendeskIds?: string[];
  partner?: string;
  current?: string;
}

export interface IMemberUpdate {
  updatedBy?: string;
  demographics?: IMemberDemographics;
  partner?: string;
  insurances?: IInsurance[];
  tags?: string[];
  cohortId?: number;
  categoryId?: number;
  zendeskId?: string;
  mrnId?: string;
  elationPlan?: string;
}

export interface IExternalIds {
  datasource: string;
  ids: IExternalId[];
}

export interface IExternalId {
  id: string;
  current: boolean;
}

interface IDatabaseExtractedDetails {
  id: string;
  carrier: string;
  current?: boolean;
  rank?: string;
  lineOfBusiness?: string;
  subLineOfBusiness?: string;
  spanDateStart?: string;
  spanDateEnd?: string;
}

function castToBoolean(s: string): boolean {
  if (!s) {
    return null;
  } else if (s.toLowerCase() === 'true') {
    return true;
  } else if (s.toLowerCase() === 'false') {
    return false;
  } else {
    return null;
  }
}

async function constructMember(
  member: Member,
  mrnQueryParam: string,
  datasourceQueryParam: string,
  txn: Transaction,
) {
  const mrn: IMedicalRecordNumber = processMrn(member);
  const insurances: IInsurance[] = processPartnerInsurance(member);

  const phones: IPhone[] = processPhones(member.phones);
  const addresses: IAddress[] = processAddresses(member.addresses);
  const emails: IEmail[] = processEmails(member.emails);
  const demographics: IMemberDemographics = processDemographic(member, phones, emails, addresses);

  const medicaid: IInsurance[] = processMedicaid(member);
  const medicare: IInsurance[] = processMedicare(member);

  // Adds the mrn ID back into the externalIds json object for backwards compatibility and filters out
  // any externalIds that may be specified in the mrnQueryParam

  const externalIds = processExternalIds(member, mrn, mrnQueryParam);

  // If we applied an externalID filter and the member returns empty externalIds, then we return null.
  if ((!mrnQueryParam && !datasourceQueryParam && isEmpty(externalIds)) || !isEmpty(externalIds)) {
    return {
      ...member,
      mrn,
      demographics,
      insurances,
      medicaid,
      medicare,
      firstName: undefined,
      middleName: undefined,
      lastName: undefined,
      dateOfBirth: undefined,
      dateOfDemise: undefined,
      sex: undefined,
      gender: undefined,
      ethnicity: undefined,
      maritalStatus: undefined,
      ssnLastFour: undefined,
      phones: undefined,
      emails: undefined,
      addresses: undefined,
      memberId: undefined,
      mrnId: undefined,
      mrnName: undefined,
      rawExternalIds: undefined,
    };
  }
  return null;
}

const processMrn = (member) => {
  return { id: member.mrnId, name: member.mrnName };
};

function processDemographic(
  member: any,
  phones: IPhone[],
  emails: IEmail[],
  addresses: IAddress[],
): IMemberDemographics {
  return {
    firstName: member.firstName,
    middleName: member.middleName,
    lastName: member.lastName,
    dateOfBirth: member.dateOfBirth,
    dateOfDemise: member.dateOfDemise,
    sex: member.sex,
    gender: member.gender,
    ethnicity: member.ethnicity,
    maritalStatus: member.maritalStatus,
    ssnLastFour: member.ssnLastFour,
    isMarkedDeceased: Boolean(member.isMarkedDeceased),
    phones,
    emails,
    addresses,
  };
}

function processInsurance(member: Member, carrierFilterFn: (string) => boolean): IInsurance[] {
  const rawExternalIds = (member as any).rawExternalIds;

  if (isEmpty(rawExternalIds)) {
    return [];
  }

  const formattedRawExternalIds: IDatabaseExtractedDetails[] = rawExternalIds.map(
    ([
      carrier,
      id,
      current,
      lineOfBusiness,
      subLineOfBusiness,
      rank,
      spanDateStart,
      spanDateEnd,
    ]) => {
      return {
        carrier,
        id,
        current: castToBoolean(current),
        lineOfBusiness,
        subLineOfBusiness,
        rank,
        spanDateStart,
        spanDateEnd,
      };
    },
  );
  const grouped = groupBy(formattedRawExternalIds, 'carrier');

  return Object.entries(grouped)
    .filter(carrierFilterFn)
    .map(([carrier, insuranceRows]) => {
      const groupedInsuranceRows: Map<string, IDatabaseExtractedDetails[]> = groupBy(
        insuranceRows,
        ({ id, current, rank }) => `${id},${current},${rank}`,
      );
      const plans = Object.entries(groupedInsuranceRows).map(
        ([_, groupedDetailsSeq]: [string, IDatabaseExtractedDetails[]]) => {
          const { id, current, rank } = groupedDetailsSeq[0];
          const mappedDetails: IInsuranceDetails[] = groupedDetailsSeq.map(
            ({ lineOfBusiness, subLineOfBusiness, spanDateStart, spanDateEnd }) => {
              return {
                lineOfBusiness,
                subLineOfBusiness,
                spanDateStart,
                spanDateEnd,
              };
            },
          );

          return {
            externalId: id,
            current,
            rank,
            details: mappedDetails,
          };
        },
      );

      return {
        carrier,
        plans,
      };
    });
}

function processMedicare(member: Member) {
  const filterFn = (_carrier) => some(STATE_IDS, (stateId) => startsWith(stateId, 'medicare'));
  return processInsurance(member, filterFn);
}

function processMedicaid(member: Member) {
  const filterFn = (_carrier) => some(STATE_IDS, (stateId) => startsWith(stateId, 'medicaid'));
  return processInsurance(member, filterFn);
}

function processPartnerInsurance(member: Member) {
  const filterFn = (datasource) => STATE_IDS.indexOf(datasource) === -1;
  return processInsurance(member, filterFn);
}

// This is a legacy function that is kept in order to support searching across multiple datasources
function processExternalIds(member: Member, mrn: IMedicalRecordNumber, mrnQueryParam: string) {
  // Each entry looks like: ["emblem", "K0212"], so we need to:
  // 1. Group by datasource
  // 2. Convert the resulting object into an array of objects that look like IExternalIds
  const grouped = groupBy((member as any).rawExternalIds, ([datasource, _]) => datasource);
  // tslint:disable-next-line: no-unnecessary-local-variable
  let externalIds = Object.entries(grouped).map(([datasource, rawExternalIds]) => ({
    datasource,
    ids: (rawExternalIds as any).map(([_, externalId]) => {
      return { id: externalId };
    }),
  }));

  // 1. Check whether the mrn.Id and mrn.Name exists.
  //    If it does exist and it does not conflict with the mrnQueryParam then add it to the externalIds array.
  // 2. If Mrn query param exists, filter for datasources that match with the query param
  const { id, name } = mrn;
  externalIds.push({ datasource: name, ids: [{ id, current: true }] });

  if (mrnQueryParam) {
    externalIds = filter(externalIds, { datasource: mrnQueryParam });
  }

  return externalIds;
}

export class Member extends BaseModel {
  static tableName = 'member';

  id!: string;
  cbhId!: number;
  partner!: string;
  cohort!: string | null;
  cohortId?: number;
  category!: string | null;
  categoryId?: number;
  externalIds!: IExternalIds[];
  insurances!: IInsurance[];
  demographics!: IMemberDemographics;
  deletedReason!: string | undefined;
  mrn!: IMedicalRecordNumber;
  mrnId!: string | undefined;
  zendeskId!: string | undefined;
  phones: IPhone[];
  emails: IEmail[];
  addresses: IAddress[];

  static get relationMappings(): RelationMappings {
    return {
      cohort: {
        relation: Model.BelongsToOneRelation,
        modelClass: Cohort,
        join: {
          from: 'member.cohortId',
          to: 'cohort.id',
        },
      },
      memberInsurance: {
        relation: Model.HasManyRelation,
        modelClass: MemberInsurance,
        join: {
          from: 'member.id',
          to: 'member_insurance.memberId',
        },
      },
      partner: {
        relation: Model.BelongsToOneRelation,
        modelClass: Partner,
        join: {
          from: 'member.partnerId',
          to: 'partner.id',
        },
      },
      mrn: {
        relation: Model.BelongsToOneRelation,
        modelClass: MedicalRecordNumber,
        join: {
          from: 'member.mrnId',
          to: 'mrn.id',
        },
      },
      demographic: {
        relation: Model.HasManyRelation,
        modelClass: MemberDemographics,
        join: {
          from: 'member.id',
          to: 'member_demographics.id',
        },
      },
      phone: {
        relation: Model.HasManyRelation,
        modelClass: Phone,
        join: {
          from: 'member.id',
          to: 'phone.id',
        },
      },
      email: {
        relation: Model.HasManyRelation,
        modelClass: Email,
        join: {
          from: 'member.id',
          to: 'email.id',
        },
      },
      address: {
        relation: Model.HasManyRelation,
        modelClass: Address,
        join: {
          from: 'member.id',
          to: 'address.id',
        },
      },
      // TODO: Add relation to Category when there is a models for it.
    };
  }

  static async create(
    partnerName: string,
    clientSource: string | undefined,
    cohortId: number | undefined,
    categoryId: number | undefined,
    mrnId: string | undefined,
    txn: Transaction,
  ) {
    const partner = await Partner.getByName(partnerName, txn);
    if (!partner) {
      throw new Error(`invalid partner name on member create [partner: ${partnerName}]`);
    }

    let mrnIdentifier;
    if (!!mrnId) {
      const mrn = await MedicalRecordNumber.create(mrnId, ELATION, txn);
      mrnIdentifier = mrn.id;
    }

    const memberToCreate: any = {
      clientSource: clientSource || null,
      cohortId,
      categoryId,
      partnerId: partner.id,
      mrnId: mrnIdentifier || null,
    };

    return this.query(txn).insertAndFetch(omitBy(memberToCreate, isNil));
  }

  // TODO: break out a separate method for getAll() and getById()
  static async get(memberId: string, txn: Transaction): Promise<Member | null> {
    const members = await Member.getMultiple({ memberIds: [memberId] }, txn);
    return head(members) || null;
  }

  static async getByZendeskId(zendeskId: string, txn: Transaction) {
    const members = await Member.getMultiple({ zendeskIds: [zendeskId] }, txn);
    return head(members) || null;
  }

  // A helper function that filters out null values in order to maintain backward compatibility with datasource query params
  static async getMultiple(filters: IMemberFilter, txn: Transaction) {
    const members = await Member.getMultipleRaw(filters, txn);
    return members.filter((member) => !isNil(member));
  }

  static async getMultipleRaw(filters: IMemberFilter, txn: Transaction) {
    if (filters.memberIds) {
      filters.memberIds.forEach((id) => {
        if (!validate(id)) {
          throw new Error(`cannot validate member id on member get [memberId: ${id}]`);
        }
      });
    }

    // TODO: Add a filter for query param for MrnId and MrnName.
    let datasource: Datasource;
    let mrn: string;
    let partner: Partner;
    let current: boolean;
    if (filters.datasource) {
      if (isMrn(filters.datasource)) {
        mrn = filters.datasource;
      } else {
        // if set as a filter, we expect datasource to exist in the table (else it throws an error)
        datasource = await Datasource.getByName(filters.datasource, txn);
      }
    }
    if (filters.partner) {
      partner = await Partner.getByName(filters.partner, txn);
      if (!partner) {
        return [];
      }
    }
    if (filters.current) {
      const checkedCurrent = castToBoolean(filters.current);
      if (checkedCurrent !== null) {
        current = checkedCurrent;
      }
    }

    const demographicsSubQuery = MemberDemographics.demographicsSubQuery(filters, txn);
    const phoneSubQuery = MemberDemographics.phoneSubQuery(filters, txn);
    const emailSubQuery = MemberDemographics.emailSubQuery(filters, txn);
    const addressSubQuery = MemberDemographics.addressSubQuery(filters, txn);

    const mdiSubQuery = MemberInsurance.query(txn)
      .select(
        'memberId',
        txn.raw(
          'array_agg(array[ds."name", "externalId", "current"::VARCHAR, "lineOfBusiness"::VARCHAR, "subLineOfBusiness", "rank"::VARCHAR, "spanDateStart"::VARCHAR, "spanDateEnd"::VARCHAR]) AS "rawExternalIds"',
        ),
      )
      .innerJoin('datasource AS ds', 'member_insurance.datasourceId', 'ds.id')
      .leftOuterJoin(
        'member_insurance_details as details',
        'member_insurance.id',
        'details.memberDatasourceId',
      )
      .modify((builder) => {
        if (datasource) {
          builder.where('ds.name', datasource.name);
        }
      })
      .modify((builder) => {
        if (filters.externalIds && !mrn) {
          builder.whereIn('externalId', filters.externalIds);
        }
      })
      .modify((builder) => {
        if (!!current) {
          builder.where({ 'member_insurance.current': current });
        }
      })
      .modify((builder) => {
        if (filters.memberIds) {
          builder.whereIn('memberId', filters.memberIds);
        }
      })
      .modify((builder) => {
        builder.whereNull('member_insurance.deletedAt');
      })
      .groupBy(['memberId']);

    const mrnSubQuery = MedicalRecordNumber.query(txn)
      .select('id', 'mrn', 'name')
      .whereNull('deletedAt');

    const results = this.query(txn)
      .select(
        'member.id AS id',
        'cbhId',
        'zendeskId',
        'p.name AS partner',
        'co.name AS cohort',
        'co.id AS cohortId',
        'cat.name AS category',
        'mrn.mrn AS mrnId',
        'mrn.name AS mrnName',
        'dem.*',
        'phone.phones',
        'email.emails',
        'address.addresses',
        'mdi.rawExternalIds',
      )
      .innerJoin('partner AS p', 'member.partnerId', 'p.id')
      .leftJoin('cohort AS co', 'member.cohortId', 'co.id')
      .leftJoin('category AS cat', 'member.categoryId', 'cat.id')
      .leftOuterJoin(txn.raw(`(${mdiSubQuery}) AS mdi`), 'member.id', 'mdi.memberId')
      .leftOuterJoin(txn.raw(`(${mrnSubQuery}) AS mrn`), 'member.mrnId', 'mrn.id')
      .leftOuterJoin(txn.raw(`(${demographicsSubQuery}) AS dem`), 'member.id', 'dem.memberId')
      .leftOuterJoin(txn.raw(`(${phoneSubQuery}) AS phone`), 'member.id', 'phone.memberId')
      .leftOuterJoin(txn.raw(`(${emailSubQuery}) as email`), 'member.id', 'email.memberId')
      .leftOuterJoin(txn.raw(`(${addressSubQuery}) as address`), 'member.id', 'address.memberId')
      .modify((builder) => {
        if (partner) {
          builder.where({ 'member.partnerId': partner.id });
        }
      })
      .modify((builder) => {
        if (filters.memberIds) {
          builder.whereIn('member.id', filters.memberIds);
        }
      })
      .modify((builder) => {
        if (filters.zendeskIds) {
          builder.whereIn('zendeskId', filters.zendeskIds);
        }
      })
      .modify((builder) => {
        if (filters.externalIds && mrn) {
          builder.whereIn('mrn.mrn', filters.externalIds);
        }
      })
      .modify((builder) => {
        // When a datasource or externalIds are specified, we filter results to just the ones where
        // those are present. But that is done in a subquery that we do a left outer join against,
        // so we need to do an additional filter here to throw out empty results (which means there
        // are no external ids with the specified datasource for the member).
        if (datasource || filters.externalIds || !!current) {
          builder.where(txn.raw('array_length(mdi."rawExternalIds", 1)'), '>', 0);
        }
      })
      .where((builder) => builder.whereNull('cohortId').orWhere('cohortId', '>', '0'))
      .whereNull('member.deletedAt');

    return results.map((member: Member) => constructMember(member, mrn, filter.datasource, txn));
  }

  static async delete(
    memberId: string,
    reason: string | undefined,
    deletedBy: string | undefined,
    txn: Transaction,
  ) {
    if (!validate(memberId)) {
      throw new Error(`cannot validate member id on member delete [memberId: ${memberId}]`);
    }
    const member = await this.query(txn).findById(memberId);
    if (!member) {
      throw new Error(`member does not exist on member delete [memberId: ${memberId}]`);
    }
    if (!!member.deletedAt) {
      return member;
    }

    const mrn = await MedicalRecordNumber.getMrn(memberId, txn);
    const elationDeleteResponse = await deletePatientIfElation(mrn);
    if (!!elationDeleteResponse && !elationDeleteResponse.success) {
      const elationId = elationDeleteResponse.elationId;
      // tslint:disable no-console
      console.log(`failed to delete elation ID: ${elationId}`);
      // tslint:enable no-console
      throw new Error(
        `failed to delete member in elation [memberId: ${memberId}, elationId: ${elationId}]`,
      );
    }

    await MemberInsurance.deleteByMember(memberId, txn);

    if (member.zendeskId) {
      const zendeskDeleteResponse = await deleteZendeskUser(member.zendeskId);
      if (
        !zendeskDeleteResponse ||
        !zendeskDeleteResponse.user ||
        zendeskDeleteResponse.user.active
      ) {
        // tslint:disable no-console
        console.log(
          `failed to delete zendesk profile for [memberId: ${memberId}, zendeskId: ${member.zendeskId}]`,
        );
        // tslint:enable no-console
        throw new Error(
          `failed to delete zendesk profile [memberId: ${memberId}, zendeskId: ${member.zendeskId}]`,
        );
      }
    }

    return this.query(txn)
      .where({ id: memberId, deletedAt: null })
      .patchAndFetchById(memberId, {
        deletedAt: new Date(),
        deletedReason: reason || null,
        deletedBy: deletedBy || null,
      });
  }

  static async deleteMemberMrn(
    memberId: string,
    reason: string | undefined,
    deletedBy: string | undefined,
    txn: Transaction,
  ) {
    if (!validate(memberId)) {
      throw new Error(`cannot validate member id on member delete [memberId: ${memberId}]`);
    }
    const member = await this.query(txn).findById(memberId);
    if (!member) {
      throw new Error(`member does not exist on member delete [memberId: ${memberId}]`);
    }
    if (!!member.deletedAt) {
      return member;
    }

    const mrn = await MedicalRecordNumber.getMrn(memberId, txn);
    const elationDeleteResponse = await deletePatientIfElation(mrn);
    if (!!elationDeleteResponse && !elationDeleteResponse.success) {
      const elationId = elationDeleteResponse.elationId;
      throw new Error(
        `failed to delete elation profile on mrn removal [memberId: ${memberId}, elationId: ${elationId}]`,
      );
    }

    try {
      const memberMrnRemoval: IMemberUpdate = { mrnId: null };
      await MedicalRecordNumber.deleteMrnIfExists(memberId, reason, txn);
      return this.query(txn).patchAndFetchById(memberId, memberMrnRemoval);
    } catch (err) {
      throw new Error(`failed to delete member mrn [memberId: ${memberId}, mrnId: ${mrn.id}]`);
    }
  }

  static async updateCategory(memberId: string, category: string, txn: Transaction) {
    const _category = await Category.getByName(category, txn);

    return this.query(txn)
      .patch({ categoryId: _category.id })
      .where({ id: memberId, deletedAt: null })
      .returning('*')
      .first();
  }

  static async updateCohort(memberId: string, cohortName: string, txn: Transaction) {
    const cohort = await Cohort.getByName(cohortName, txn);

    return this.query(txn)
      .patch({ cohortId: cohort.id })
      .where({ id: memberId, deletedAt: null })
      .returning('*')
      .first();
  }

  static async update(
    memberId: string,
    memberUpdateInput: IMemberUpdate,
    txn: Transaction,
  ): Promise<Member> {
    if (!validate(memberId)) {
      throw new Error(`invalid memberId on member update [memberId: ${memberId}]`);
    }
    const member = await this.query(txn).findById(memberId);
    if (!member) {
      throw new Error(`member does not exist on member update [memberId: ${memberId}]`);
    }

    const memberUpdate = omitBy({ zendeskId: memberUpdateInput.zendeskId }, isNil);

    const memberDemographicsUpdate: IUpdateMemberDemographics =
      memberUpdateInput.demographics &&
      omitBy(
        {
          ...memberUpdateInput.demographics,
          addresses: undefined,
          emails: undefined,
          phones: undefined,
        },
        isNil,
      );

    const memberAddressUpdate: IAddress[] =
      memberUpdateInput.demographics && memberUpdateInput.demographics.addresses;
    const memberEmailUpdate: IEmail[] =
      memberUpdateInput.demographics && memberUpdateInput.demographics.emails;
    const memberPhoneUpdate: IPhone[] =
      memberUpdateInput.demographics && memberUpdateInput.demographics.phones;

    const updatePromises = [];

    if (!isEmpty(memberDemographicsUpdate))
      updatePromises.push(
        MemberDemographics.updateAndSaveHistory(memberId, memberDemographicsUpdate, txn),
      );

    if (memberAddressUpdate && memberAddressUpdate.length)
      updatePromises.push(Address.createAllNew(memberId, memberAddressUpdate, txn));

    if (memberEmailUpdate && memberEmailUpdate.length)
      updatePromises.push(Email.createAllNew(memberId, memberEmailUpdate, txn));

    if (memberPhoneUpdate && memberPhoneUpdate.length)
      updatePromises.push(Phone.createAllNew(memberId, memberPhoneUpdate, txn));

      const insurancesUpdates =
      memberUpdateInput.insurances &&
      memberUpdateInput.insurances.map(async (insurance) => {
        const { carrier, plans } = insurance;
        const updatedBy = memberUpdateInput.updatedBy;
        const consolidateAndUpdateDetailsInput: IConsolidateAndUpdateDetailsInput = { memberId, carrier, plans };
        
        await MemberInsurance.updateInsurances({ memberId, carrier, plans }, updatedBy, txn);
        await MemberInsurance.consolidateInsurancesAndUpdateDetails(consolidateAndUpdateDetailsInput, txn);
        return MemberInsurance.getInsurancesByDatasource(memberId, carrier, txn);
      });

    await Promise.all(updatePromises.concat(insurancesUpdates));

    return this.query(txn).patchAndFetchById(memberId, memberUpdate);
  }
}
