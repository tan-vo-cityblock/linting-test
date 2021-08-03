import { Model, RelationMappings } from 'objection';
import BaseModel from './base-model';
import { MarketMeasure } from './market-measure';

/* tslint:disable:member-ordering */
export class Market extends BaseModel {
  static tableName = 'market';

  id: string;
  name: string;
  slug: string;

  static get relationMappings(): RelationMappings {
    return {
      marketMeasure: {
        relation: Model.HasManyRelation,
        modelClass: MarketMeasure,
        join: {
          from: 'market.id',
          to: 'market_measure.marketId',
        },
      },
    };
  }
}
/* tslint:enable:member-ordering */
