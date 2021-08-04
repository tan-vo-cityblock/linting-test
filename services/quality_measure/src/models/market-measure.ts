import { Model, RelationMappings } from 'objection';
import BaseModel from './base-model';
import { Market } from './market';
import { Measure } from './measure';

/* tslint:disable:member-ordering */
export class MarketMeasure extends BaseModel {
  static tableName = 'market_measure';

  id: number;
  marketId: string;
  measureId: string;

  static get relationMappings(): RelationMappings {
    return {
      market: {
        relation: Model.BelongsToOneRelation,
        modelClass: Market,
        join: {
          from: 'market_measure.marketId',
          to: 'market.id',
        },
      },
      measure: {
        relation: Model.BelongsToOneRelation,
        modelClass: Measure,
        join: {
          from: 'market_measure.measureId',
          to: 'measure.id',
        },
      },
    };
  }
}
/* tslint:enable:member-ordering */
