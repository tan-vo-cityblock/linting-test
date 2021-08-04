import { isEmpty, keyBy, omit, uniq } from 'lodash';
import { raw, Model, RelationMappings, Transaction } from 'objection';
import { isDateInRangeExclusive } from '../util/helper';
import BaseModel from './base-model';
import { Market } from './market';
import { IMeasurePerformanceRange, Measure, MeasureType } from './measure';
import { MemberMeasureSource, SourceNames } from './member-measure-source';
import { MemberMeasureStatus } from './member-measure-status';

type Dictionary<T> = import('lodash').Dictionary<T>;
export interface IMemberMeasureRequest {
  id?: number;
  code?: string;
  rateId?: number | null;
  userId?: string | null;
  status: string;
  setOn: string;
  performanceYear?: number;
  reason?: string | null;
}

export interface IMemberMeasureResponse {
  id: number;
  displayName: string;
  status: string;
  type: string;
}

interface IMemberMeasureReqPair {
  prevMemberMeasure: MemberMeasure;
  memberMeasureToAdd: Partial<MemberMeasure>;
}

/* tslint:disable:member-ordering */
export class MemberMeasure extends BaseModel {
  static tableName = 'member_measure';

  id: number;
  memberId: string;
  measureId: number;
  sourceId: number;
  statusId: number;
  reason: string | null;
  setOn: Date;
  performanceYear: number | null;
  lastConfirmed: Date;
  userId: string;
  measure: Measure;
  memberMeasureStatus: MemberMeasureStatus;
  market: Market[];

  static get relationMappings(): RelationMappings {
    return {
      measure: {
        relation: Model.BelongsToOneRelation,
        modelClass: Measure,
        join: {
          from: 'member_measure.measureId',
          to: 'measure.id',
        },
      },
      memberMeasureSource: {
        relation: Model.BelongsToOneRelation,
        modelClass: MemberMeasureSource,
        join: {
          from: 'member_measure.sourceId',
          to: 'member_measure_source.id',
        },
      },
      memberMeasureStatus: {
        relation: Model.BelongsToOneRelation,
        modelClass: MemberMeasureStatus,
        join: {
          from: 'member_measure.statusId',
          to: 'member_measure_status.id',
        },
      },
      market: {
        relation: Model.ManyToManyRelation,
        modelClass: Market,
        join: {
          from: 'member_measure.measureId',
          through: {
            from: 'market_measure.measureId',
            to: 'market_measure.marketId',
          },
          to: 'market.id',
        },
      },
    };
  }

  private static defaultRateIdIfNull(rateId: number | null) {
    return rateId ? rateId : 0;
  }
  private static makeStringCodeAndRateId(code: string, rateId: number) {
    return `${code}${this.defaultRateIdIfNull(rateId)}`;
  }

  private static checkStatusForMeasure(measure: Measure, statusName: string) {
    const invalidStatusForMeasure =
      measure.type === MeasureType.outcome && statusName === 'provClosed';

    if (invalidStatusForMeasure) {
      throw new Error(
        `Invalid measure status: '${statusName}' for outcome measure: '${measure.code}'`,
      );
    } else {
      return;
    }
  }

  private static getMeasureFromIdentifiers(
    measureId: number,
    code: string,
    rateId: number,
    allMeasuresByIds: Dictionary<Measure>,
    allMeasuresByCodeRateIds: Dictionary<Measure>,
  ) {
    return measureId && measureId.toString() in allMeasuresByIds
      ? allMeasuresByIds[measureId.toString()]
      : allMeasuresByCodeRateIds[this.makeStringCodeAndRateId(code, rateId)];
  }

  private static async getAllMemberMeasureStatusesByNames(
    measureStatusReqs: IMemberMeasureRequest[],
    txn: Transaction,
  ) {
    const measureStatusReqsStatusNames = uniq(
      measureStatusReqs.map((measureStatusReq) => measureStatusReq.status),
    );

    const allMemberMeasureStatuses = keyBy(
      await MemberMeasureStatus.getAll(txn),
      (memberMeasureStatus) => memberMeasureStatus.name,
    );

    const allMemberMeasureStatusNames = Object.keys(allMemberMeasureStatuses);
    const measureStatusReqsStatusNamesAreValid = measureStatusReqsStatusNames.every((name) => {
      return allMemberMeasureStatusNames.includes(name);
    });

    // TODO: Add check to make sure certain sources can make only certain status changes.
    if (!measureStatusReqsStatusNamesAreValid) {
      throw new Error(
        `Invalid measure status request status names: ${measureStatusReqsStatusNames}`,
      );
    }

    return allMemberMeasureStatuses;
  }

  private static async getMeasureStatusRequestSourceId(sourceName: string, txn: Transaction) {
    const memberMeasureSource = await MemberMeasureSource.getByName(sourceName, txn);

    if (memberMeasureSource) {
      return memberMeasureSource.id;
    } else {
      throw new Error(`Invalid measure status request source name: ${sourceName}`);
    }
  }

  private static filterMemberMeasureReqsForTrackedMeasures(
    memberMeasureReqs: IMemberMeasureRequest[],
    trackedMeasureIds: string[],
    trackedMeasureCodeRateIds: string[],
  ) {
    return memberMeasureReqs.filter((measureStatusReq) => {
      const { id, code, rateId } = measureStatusReq;
      const idIsAreTracked = id && trackedMeasureIds.includes(id.toString());
      const codeRateIdsAreTracked =
        code &&
        rateId !== undefined &&
        trackedMeasureCodeRateIds.includes(this.makeStringCodeAndRateId(code, rateId));
      return idIsAreTracked || codeRateIdsAreTracked;
    });
  }

  private static async getMostRecentMemberMeasureForYearGraphFetch(
    memberId: string,
    measureId: number,
    measureSetOnYearRange: IMeasurePerformanceRange,
    txn: Transaction,
  ) {
    return this.query(txn)
      .whereNull('deletedAt')
      .andWhere('memberId', memberId)
      .andWhere('measureId', measureId)
      .andWhere('setOn', '>=', measureSetOnYearRange.setOnYearStart)
      .andWhere('setOn', '<', measureSetOnYearRange.setOnYearEnd)
      .orderBy('setOn', 'desc')
      .limit(1)
      .first()
      .withGraphFetched('[memberMeasureStatus, measure]');
  }

  private static async pairPrevMemberMeasuresWithMemberMeasureReqs(
    memberId: string,
    sourceName: string,
    memberMeasureReqs: IMemberMeasureRequest[],
    txn: Transaction,
  ): Promise<IMemberMeasureReqPair[]> {
    const allMeasures = await Measure.getAll(txn);
    const allMeasuresByIds = keyBy(allMeasures, (measure) => measure.id.toString());
    const allMeasuresByCodeRateIds = keyBy(allMeasures, (measure) =>
      this.makeStringCodeAndRateId(measure.code, measure.rateId),
    );

    const filteredMemberMeasureReqs = this.filterMemberMeasureReqsForTrackedMeasures(
      memberMeasureReqs,
      Object.keys(allMeasuresByIds),
      Object.keys(allMeasuresByCodeRateIds),
    );

    const allMemberMeasureStatuses = await this.getAllMemberMeasureStatusesByNames(
      filteredMemberMeasureReqs,
      txn,
    );
    const memberMeasureSourceId = await this.getMeasureStatusRequestSourceId(sourceName, txn);

    const memberMeasureReqPairs = filteredMemberMeasureReqs.map(async (memberMeasureReq) => {
      const { id, code, rateId, status, setOn, userId, reason, performanceYear } = memberMeasureReq;
      const measureToAdd = this.getMeasureFromIdentifiers(
        id,
        code,
        rateId,
        allMeasuresByIds,
        allMeasuresByCodeRateIds,
      );

      const memberMeasureToAdd: Partial<MemberMeasure> = {
        memberId,
        measureId: measureToAdd.id,
        sourceId: memberMeasureSourceId,
        statusId: allMemberMeasureStatuses[status].id,
        setOn: new Date(setOn), // validated in add-member-measures.ts to be ISO8601 UTC string
        performanceYear,
        userId,
        reason,
      };

      this.checkStatusForMeasure(measureToAdd, status);

      const prevMemberMeasure = await this.getMostRecentMemberMeasureForYearGraphFetch(
        memberMeasureToAdd.memberId,
        memberMeasureToAdd.measureId,
        measureToAdd.getPerformanceRangeForDate(memberMeasureToAdd.setOn),
        txn,
      );

      return {
        prevMemberMeasure,
        memberMeasureToAdd,
      };
    });

    return Promise.all(memberMeasureReqPairs);
  }

  static async insertOrPatchMemberMeasures(
    memberId: string,
    sourceName: string,
    memberMeasureReqs: IMemberMeasureRequest[],
    txn: Transaction,
  ): Promise<MemberMeasure[]> {
    try {
      const memberMeasureReqPairs = await this.pairPrevMemberMeasuresWithMemberMeasureReqs(
        memberId,
        sourceName,
        memberMeasureReqs,
        txn,
      );

      const processedMemberMeasures = memberMeasureReqPairs.map(async (memberMeasureReqPair) => {
        const { prevMemberMeasure, memberMeasureToAdd } = memberMeasureReqPair;

        // TODO: Look into breaking up below if statements to allow for batch inserts and improve performance

        // if no previous member measure exists, then insert the new member measure only if measure is being added from `able` source.
        // `able` is currently the only source that opens / adds measures for the first time.
        // or if prev member measure exists with different status and new member measure has a more recent setOn date in the same year, then add new member measure

        const isFirstTimeMeasureFromAble = !prevMemberMeasure && sourceName === SourceNames.able;
        const isFollowOnMeasureWithDiffStatusAndNewerSetOn =
          prevMemberMeasure &&
          prevMemberMeasure.statusId !== memberMeasureToAdd.statusId &&
          prevMemberMeasure.setOn < memberMeasureToAdd.setOn;
        const isFollowOnMeasureWithSameStatus =
          prevMemberMeasure && prevMemberMeasure.statusId === memberMeasureToAdd.statusId;

        if (isFirstTimeMeasureFromAble || isFollowOnMeasureWithDiffStatusAndNewerSetOn) {
          return this.query(txn)
            .insert(memberMeasureToAdd)
            .returning('*')
            .withGraphFetched('[memberMeasureStatus, measure]');
        } else if (isFollowOnMeasureWithSameStatus) {
          // if member measure statuses are the same in the same year, simply update lastConfirmed column
          const confirmed = new Date().toISOString(); // to guarantee the current timestamp is ISO UTC instead of relying on system timezone.
          return this.query(txn)
            .patch({ lastConfirmed: new Date(confirmed) })
            .where('id', prevMemberMeasure.id)
            .returning('*')
            .first()
            .withGraphFetched('[memberMeasureStatus, measure]');
        } else {
          return Promise.resolve(prevMemberMeasure);
        }
      });
      return Promise.all(processedMemberMeasures);
    } catch (e) {
      console.error('Error adding member measures', e);
      throw e;
    }
  }

  static async getMemberMeasures(memberId: string, marketSlug: string, txn: Transaction) {
    const distinctMemberMeasuresForMarket = await this.query(txn)
      .withGraphJoined('[measure, market]')
      .distinctOn('member_measure.measureId')
      .select('member_measure.measureId')
      .whereNull('member_measure.deletedAt')
      .whereNull('measure.deletedAt')
      .whereNull('market.deletedAt')
      .andWhere('member_measure.memberId', memberId)
      .andWhere('market.slug', marketSlug);

    const currentDatetime = new Date();

    const mostRecentMemberMeasures = distinctMemberMeasuresForMarket.map(
      async (distinctMemberMeasure) => {
        return this.getMostRecentMemberMeasureForYearGraphFetch(
          memberId,
          distinctMemberMeasure.measureId,
          distinctMemberMeasure.measure.getPerformanceRangeForDate(currentDatetime),
          txn,
        );
      },
    );

    return Promise.all(mostRecentMemberMeasures);
  }

  static async getMemberMeasuresFromQuery(
    measureIds: number[] | null,
    measureStatusIds: number[],
    memberIds: string[],
    txn: Transaction,
  ) {
    const measureIdsIsNotEmpty = !isEmpty(measureIds);

    const filteredResultsQuery = this.query(txn)
      .with('filteredMemberMeasures', (builder) => {
        builder
          .from(
            raw(
              `(SELECT *, 
            ROW_NUMBER() OVER (PARTITION BY "memberId", "measureId" ORDER BY "setOn" DESC) AS "latest" 
            FROM member_measure) AS partitioned`,
            ),
          )
          .whereNull('deletedAt')
          .andWhere('latest', 1)
          .whereIn('memberId', memberIds)
          .whereIn('statusId', measureStatusIds);

        if (measureIdsIsNotEmpty) {
          builder.whereIn('measureId', measureIds);
        }
      })
      .select('*')
      .from('filteredMemberMeasures')
      .withGraphFetched('measure');

    if (measureIdsIsNotEmpty) {
      // TODO: Add db change / migration to represent below comment.

      // Measure ids are grouped based on grouping for display in Commons. Groupings here are a subset of groupings by "measure.categoryId" in db.
      // The query below needs to return member - measure pairings for each measure to filter on. So if a measure to filter on does not exist on the memberId,
      // the query should exclude that memberId.
      // Therefore we use the measure groupings to relax requriement on count of same memberId that must be paired to each measure to filter on.
      // i.e. For each array of known grouped measures, if array of measures to filter on contains all of the known grouped measures,
      // we reduce the required count of memberIds that must pair with each measure to filter on.
      const measuresGroupedWellVisit = [2, 3];
      const measuresGroupedFlu = [16, 17];
      const groupWellVisit: number = measuresGroupedWellVisit.every((id) => measureIds.includes(id))
        ? measuresGroupedWellVisit.length - 1
        : 0;
      const groupFlu: number = measuresGroupedFlu.every((id) => measureIds.includes(id))
        ? measuresGroupedFlu.length - 1
        : 0;
      const memberIdRequiredCountForQuery = measureIds.length - (groupFlu + groupWellVisit);

      filteredResultsQuery.whereIn('memberId', (builder) => {
        builder
          .select('memberId')
          .from('filteredMemberMeasures')
          .groupBy('memberId')
          .havingRaw('count("memberId") = ?', memberIdRequiredCountForQuery);
      });
    }

    const filteredResults = await filteredResultsQuery;

    const currentDatetime = new Date();

    const resultsFilteredForCurrentYear = filteredResults.filter((memberMeasure) => {
      const performanceRange = memberMeasure.measure.getPerformanceRangeForDate(currentDatetime);
      const performanceRangeStart = new Date(performanceRange.setOnYearStart);
      const performanceRangeEnd = new Date(performanceRange.setOnYearEnd);

      return isDateInRangeExclusive(
        performanceRangeStart,
        memberMeasure.setOn,
        performanceRangeEnd,
      );
    });

    return Promise.resolve(resultsFilteredForCurrentYear.map((result) => omit(result, 'latest')));
  }
}
/* tslint:enable:member-ordering */
