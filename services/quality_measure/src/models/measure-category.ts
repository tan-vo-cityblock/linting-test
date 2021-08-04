import { Model, RelationMappings } from 'objection';
import BaseModel from './base-model';
import { Measure } from './measure';

/* tslint:disable:member-ordering */
export class MeasureCategory extends BaseModel {
  static tableName = 'measure_category';

  id: number;
  name: string;

  static get relationMappings(): RelationMappings {
    return {
      measure: {
        relation: Model.HasManyRelation,
        modelClass: Measure,
        join: {
          from: 'measure_category.id',
          to: 'measure.categoryId',
        },
      },
    };
  }
}
/* tslint:enable:member-ordering */
