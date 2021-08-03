import { compareAsc, isBefore, parseISO, sub } from 'date-fns';
import { flatMap, groupBy, head, isEmpty, isNil, omitBy, sortBy } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import { processInsurance, reduceInsurance } from '../util/insurance';
import BaseModel from './base-model';
import { Datasource } from './datasource';
import { Member } from './member';
import { IInsuranceDetails, MemberInsuranceDetails } from './member-insurance-details';

export const STATE_IDS: string[] = [
  'medicaidNC',
  'medicareNC',
  'medicaidNY',
  'medicareNY',
  'medicaidCT',
  'medicareCT',
  'medicaidMA',
  'medicareMA',
  'medicaidDC',
  'medicareDC',
];

export interface IConsolidateAndUpdateDetailsInput {
  memberId: string;
  carrier: string;
  plans: IInsurancePlan[];
}

export interface IUpdatedInsuranceStatus {
  externalId: string;
  detailsCreated: number;
  detailsUpdated: number;
  detailsDeleted: number;
}

export interface MemberInsuranceMapping {
  memberId: string,
  externalId: string,
  carrier: string,
  current?: boolean,
}

export interface IUpdateInsurances {
  memberId: string;
  carrier: string;
  plans: IInsurancePlan[];
}

export interface IInsurance {
  carrier: string;
  plans: IInsurancePlan[];
}

export interface IInsurancePlan {
  externalId: string;
  rank?: string;
  current?: boolean;
  details?: IInsuranceDetails[];
}

export class MemberInsurance extends BaseModel {
  static tableName = 'member_insurance';

  id!: string;
  memberId!: string;
  externalId!: string;
  rank: string;
  datasourceId!: number;
  datasource?: string;
  current: boolean;
  deletedReason: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'member_insurance.memberId',
          to: 'member.id',
        },
      },
      details: {
        relation: Model.HasManyRelation,
        modelClass: MemberInsuranceDetails,
        filter: (query) =>
          query
            .clearSelect()
            .select('id', 'spanDateStart', 'spanDateEnd', 'lineOfBusiness', 'subLineOfBusiness'),
        join: {
          from: 'member_insurance.id',
          to: 'member_insurance_details.memberDatasourceId',
        },
      },
    };
  }

  static modifiers = {
    defaultSelects(query) {
      query.select('externalId', 'rank', 'current');
    },
  };

  static async createInsurances(
    memberId: string,
    insurances: IInsurance[],
    txn: Transaction,
  ): Promise<MemberInsurance[][]> {
    if (isEmpty(insurances)) {
      return null;
    }

    return Promise.all(
      insurances.map(async (insurance) => {
        const { carrier, plans }: IInsurance = insurance;
        return this.createInsurance(memberId, carrier, plans, txn);
      }),
    );
  }

  static async createInsurance(
    memberId: string,
    carrier: string,
    plans: IInsurancePlan[],
    txn: Transaction,
  ): Promise<MemberInsurance[]> {
    if (isEmpty(plans)) {
      return null;
    }
    const datasource = await Datasource.getByName(carrier, txn);
    const insurances = plans.map((plan) => {
      return {
        memberId,
        externalId: plan.externalId,
        datasourceId: datasource.id,
        rank: plan.rank,
        current: plan.current,
        details: plan.details,
      };
    });

    // TODO: relatedQuery here to insert insurance details on insurance update
    return this.query(txn).insertGraphAndFetch(insurances);
  }

  static async createOrAppendInsuranceAndDetails(
    memberId: string,
    carrier: string,
    updatedInsuranceAndDetails: IInsurancePlan,
    txn: Transaction,
  ) {
    const datasource = await Datasource.getByName(carrier, txn);
    const memberInsurance: MemberInsurance | null = await this.query(txn).findOne({
      externalId: updatedInsuranceAndDetails.externalId,
      datasourceId: datasource.id,
      deletedAt: null,
    });

    if (!!memberInsurance) {
      // append details
      // TODO: should we consolidate details before this?
      return MemberInsuranceDetails.append(carrier, updatedInsuranceAndDetails, txn);
    } else {
      // create insurance
      return this.createInsurance(memberId, carrier, [updatedInsuranceAndDetails], txn);
    }
  }

  static async getByMember(memberId: string, txn: Transaction): Promise<IInsurance[]> {
    const activeInsurancesWithDetails = await MemberInsurance.query(txn)
      .select('ds.name as carrier')
      .modify('defaultSelects')
      .whereNull('member_insurance.deletedAt')
      .andWhere({ memberId })
      .innerJoin('datasource AS ds', 'member_insurance.datasourceId', 'ds.id')
      .eager('details')
      .modifyEager('details', (builder) => builder.whereNull('deletedAt'));

    return processInsurance(activeInsurancesWithDetails as any[]);
  }

  static async getByExternalId(externalId: string, datasourceName: string, txn: Transaction) {
    const datasource = await Datasource.getByName(datasourceName, txn);
    // Assumes uniqueness constraint on datasourceId and externalId
    return this.query(txn)
      .where({ externalId, datasourceId: datasource.id, deletedAt: null })
      .first();
  }

  static async getMemberInsuranceMapping(
    filter: { memberId?: string; externalId?: string; carrier?: string },
    txn: Transaction,
  ): Promise<MemberInsuranceMapping[]> {
    return this.query(txn)
      .select('memberId', 'externalId', 'datasource.name as carrier', 'current')
      .join('datasource', 'datasource.id', 'datasourceId')
      .where('member_insurance.deletedAt', null)
      .modify((builder) => {
        const parsedFilter = omitBy({ ...filter, name: filter.carrier, carrier: undefined }, isNil);
        if (!isEmpty(parsedFilter)) {
          builder.where(parsedFilter);
        }
      }) as any;
  }

  static async getInsurancesByDatasource(
    memberId: string,
    datasourceName: string,
    txn: Transaction,
  ): Promise<MemberInsurance[]> {
    const datasource = await Datasource.getByName(datasourceName, txn);
    return this.query(txn).where({ memberId, datasourceId: datasource.id, deletedAt: null });
  }

  static async deleteByMember(memberId: string, txn: Transaction) {
    return this.query(txn).where({ memberId, deletedAt: null }).patch({ deletedAt: new Date() });
  }

  static async deleteByInsurance(
    memberId: string,
    insuranceId: string,
    deletedReason: string | undefined,
    txn: Transaction,
  ) {
    return this.query(txn)
      .patch({ updatedAt: new Date(), deletedAt: new Date(), deletedReason })
      .where({ memberId, externalId: insuranceId, deletedAt: null })
      .returning('*')
      .first();
  }

  /**
   * This function does not allow you to update the carrier of an insurance type
   */
  static async updateInsurances(
    updateInsurances: IUpdateInsurances, 
    updatedBy: string | null,
    txn: Transaction,
  ): Promise<MemberInsurance[]> {
    const { memberId, carrier, plans } = updateInsurances;

    return Promise.all(
      plans.map((plan: IInsurancePlan) => this.updateOrCreateInsurance(memberId, carrier, plan, updatedBy, txn)),
    );
  }

  // This function does not allow you to update the details for an insurance ID
  static async updateOrCreateInsurance(
    memberId: string,
    carrier: string,
    plan: IInsurancePlan,
    updatedBy: string | null,
    txn: Transaction,
  ): Promise<MemberInsurance> {
    const datasource = await Datasource.getByName(carrier, txn);
    const memberInsurance: MemberInsurance | null = await this.query(txn).findOne({
      externalId: plan.externalId,
      datasourceId: datasource.id,
      deletedAt: null,
    });

    if (!!memberInsurance) {
      const updatedPlan = omitBy({ ...plan, updatedBy, details: undefined }, isNil);
      return this.query(txn)
        .patch(updatedPlan)
        .where({
          memberId,
          datasourceId: datasource.id,
          externalId: updatedPlan.externalId,
          deletedAt: null,
        })
        .returning('*')
        .first();
    } else {
      const createdInsurances = await this.createInsurance(memberId, carrier, [plan], txn);
      // We know we are only creating one insurance here
      return createdInsurances[0];
    }
  }

  static async getLatestInsurance(memberId: string, txn): Promise<IInsurance> {
    const activeInsurance = await MemberInsurance.query(txn)
      .select('ds.name as carrier')
      .modify('defaultSelects')
      .innerJoin('datasource AS ds', 'member_insurance.datasourceId', 'ds.id')
      .eager('details')
      .modify((builder) => {
        builder.where('current', true);
        builder.andWhere('memberId', memberId);
        // TODO: add once we have ranks for all insurance ids, rank should determine the correct insurance
        // builder.andWhere('member_insurance.rank', 'primary');
        builder.whereNotIn('ds.name', STATE_IDS);
        builder.whereNull('member_insurance.deletedAt');
      })
      .modifyEager('details', builder => builder.whereNull('deletedAt'))
      .first();

    if (!activeInsurance) {
      return Promise.resolve(null);
    }

    return head(processInsurance([activeInsurance] as any[]));
  }

  static async consolidateInsurancesAndUpdateDetails(
    consolidateAndUpdateDetailsInput: IConsolidateAndUpdateDetailsInput,
    txn: Transaction
  ): Promise<IUpdatedInsuranceStatus[]> {
    const { memberId, carrier, plans } = consolidateAndUpdateDetailsInput;
    const consolidatedInsurances: IInsurancePlan[] = reduceInsurance(plans);
  
    return Promise.all(consolidatedInsurances.map(async (currInsurance: IInsurancePlan) => {
      const { rank, current } = currInsurance;
  
      const requestExternalId: string = currInsurance.externalId;
      const requestDetailsSeq: IInsuranceDetails[] = currInsurance.details;
      const existingDetailsSeq: IInsuranceDetails[] = await MemberInsuranceDetails.getByExternalId(requestExternalId, carrier, txn);
  
      // TODO: what do we do when id has existing null span starts (in db)?
      // possible solution: remove all member insurance details records with empty span date starts
      const sortedExistingDetailsSeq: IInsuranceDetails[] = sortBy(existingDetailsSeq, (insurance) =>
        new Date(insurance.spanDateStart).getTime(),
      );
      
      const mappedRequestDetails = requestDetailsSeq.map(details => idDetailsCombiner(requestExternalId, details));
      const databaseDetailsMap = createDatabaseDetailsMap(requestExternalId, sortedExistingDetailsSeq);
      
      const detailsToBePatched: IInsuranceDetails[] = [];
      const detailsToBeCreated: IInsuranceDetails[] = [];
      for (const requestDetailsIter of mappedRequestDetails) {
        const { mapKey } = requestDetailsIter;
        const requestDetails = requestDetailsIter.details;
  
        const existingDetailsSeqFromMap = databaseDetailsMap.get(mapKey);
        if (existingDetailsSeqFromMap && existingDetailsSeqFromMap.length) {
          const existingDetails = existingDetailsSeqFromMap.shift();
          const { spanDateEnd: reqSpanDateEnd, spanDateStart: reqSpanDateStart } = requestDetails;
  
          const requestDetailsForSpanComparison = {
            ...requestDetails,
            spanDateStart: reqSpanDateStart && parseISO(reqSpanDateStart).toISOString(),
            spanDateEnd: reqSpanDateEnd && parseISO(reqSpanDateEnd).toISOString(),
          }
  
          if (!spanDatesEqual(requestDetailsForSpanComparison, existingDetails)) {
            const updatedSpanDateStart = isOlderThan3YearsAgo(existingDetails.spanDateStart) ? 
              existingDetails.spanDateStart :
              reqSpanDateStart;
  
            detailsToBePatched.push({
              ...existingDetails,
              spanDateStart: updatedSpanDateStart,
              spanDateEnd: reqSpanDateEnd
            }); 
          }
        } else {
          detailsToBeCreated.push(requestDetails);
        }
      }
  
      await Promise.all(detailsToBePatched.map(async details => MemberInsuranceDetails.update(details, txn)));
  
      const plans: IInsurancePlan = { rank, current, externalId: requestExternalId, details: detailsToBeCreated };
      await MemberInsurance.createOrAppendInsuranceAndDetails(memberId, carrier, plans, txn);
  
      const idsToDelete = flatMap(
        Array.from(databaseDetailsMap.values()),
        (detailsSeq: IInsuranceDetails[]) =>
          detailsSeq.map((details: IInsuranceDetails) => details.id)
      );
  
      await MemberInsuranceDetails.bulkDelete(idsToDelete, txn);
  
      return {
        externalId: requestExternalId,

        detailsCreated: detailsToBeCreated.length,
        detailsUpdated: detailsToBePatched.length,
        detailsDeleted: idsToDelete.length
      };
    }));
  }
}

function isOlderThan3YearsAgo(isoSpanDate: string): boolean {
  const convertedDate = new Date(isoSpanDate);
  const checkDate = sub(new Date(), { years: 3 });
  return isBefore(convertedDate, checkDate);
}

function idDetailsCombiner(externalId: string, details: IInsuranceDetails) {
  const mapKey = `${externalId},${details.lineOfBusiness},${details.subLineOfBusiness}`;
  return { mapKey, externalId, details }
};

function createDatabaseDetailsMap(externalId: string, detailsSeq: IInsuranceDetails[]): Map<string, IInsuranceDetails[]> {
  const groupedDetails = groupBy(detailsSeq, 
    (details: IInsuranceDetails) => `${externalId},${details.lineOfBusiness},${details.subLineOfBusiness}`
  );
  return new Map(Object.entries(groupedDetails));
};

function spanDatesEqual(details1: IInsuranceDetails, details2: IInsuranceDetails): boolean {
  const { spanDateStart: spanDateStart1, spanDateEnd: spanDateEnd1 } = details1;
  const { spanDateStart: spanDateStart2, spanDateEnd: spanDateEnd2 } = details2;

  const formattedSpanDateStart1 = new Date(spanDateStart1);
  const formattedSpanDateStart2 = new Date(spanDateStart2);
  const formattedSpanDateEnd1 = new Date(spanDateEnd1);
  const formattedSpanDateEnd2 = new Date(spanDateEnd2);

  const spanStartDatesMatch = compareAsc(formattedSpanDateStart1, formattedSpanDateStart2) === 0;
  const spanEndDatesMatch = compareAsc(formattedSpanDateEnd1, formattedSpanDateEnd2) === 0;

  return spanStartDatesMatch && spanEndDatesMatch;
}