import { isNil, omitBy, some } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import validate from 'uuid-validate';
import { isEmpty } from 'lodash';
import BaseModel from './base-model';
import { IInsurancePlan, MemberInsurance } from './member-insurance';
import { Datasource } from './datasource';
import { processInsuranceDetails } from '../util/insurance';

export interface IInsuranceDetails {
  id?: string;
  lineOfBusiness?: string;
  subLineOfBusiness?: string;
  spanDateStart?: string;
  spanDateEnd?: string;
}

export class MemberInsuranceDetails extends BaseModel {
  static tableName = 'member_insurance_details';
  id!: string;
  memberDatasourceId!: string; // TODO: rename this field to memberInsuranceId
  spanDateStart: string;
  spanDateEnd: string;
  lineOfBusiness: string;
  subLineOfBusiness: string;
  deletedReason: string;

  static get relationMappings(): RelationMappings {
    return {
      memberDatasourceIdentifier: {
        relation: Model.BelongsToOneRelation,
        modelClass: MemberInsurance,
        join: {
          from: 'member_insurance_details.memberDatasourceId',
          to: 'member_insurance.id',
        },
      },
    };
  }

  static async update(updatedDetails: IInsuranceDetails, txn: Transaction) {
    const { id, lineOfBusiness, subLineOfBusiness, spanDateStart, spanDateEnd } = updatedDetails;
    if (!validate(id)) {
      throw new Error(`attemping to update member insurance details with invalid id [id: ${id}]`);
    }

    return this.query(txn)
      .patch({
        lineOfBusiness,
        subLineOfBusiness,
        spanDateStart,
        spanDateEnd,
      })
      .where({ id })
      .returning('*')
      .first();
  }

  static async bulkDelete(idsToDelete: string[], txn: Transaction) {
    if (isEmpty(idsToDelete)) {
      return null;
    } else if (some(idsToDelete, (id: string) => !validate(id))) {
      throw new Error(
        `attemping to delete member insurance details with invalid id [ids: ${idsToDelete}]`,
      );
    }

    return this.query(txn)
      .patch({ deletedAt: new Date() })
      .whereIn('id', idsToDelete)
      .returning('*')
      .first();
  }

  static async append(
    carrier: string,
    updatedPlan: IInsurancePlan,
    txn: Transaction,
  ) {
    const { externalId, rank, current, details }: IInsurancePlan = updatedPlan;
    const datasource = await Datasource.getByName(carrier, txn);

    const rankOrCurrentUpdate = omitBy({ rank, current }, isNil);
    const memberInsurance: MemberInsurance = await MemberInsurance.query(txn)
      .patch(rankOrCurrentUpdate)
      .where({ externalId, datasourceId: datasource.id })
      .returning('*')
      .first();

    return memberInsurance.$relatedQuery('details', txn).insertAndFetch(details as Partial<MemberInsuranceDetails>[]);
  }

  static async getByExternalId(
    externalId: string,
    datasourceName: string,
    txn: Transaction,
  ): Promise<IInsuranceDetails[]> {
    const memberInsurance: MemberInsurance = await MemberInsurance.getByExternalId(
      externalId,
      datasourceName,
      txn,
    );

    const relatedDetails = memberInsurance
      .$relatedQuery('details', txn)
      .where({ deletedAt: null })
      .returning('*') as any;
    return processInsuranceDetails(relatedDetails);
  }
}
