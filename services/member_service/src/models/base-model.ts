import { Model } from 'objection';

/* tslint:disable:check-model-variable */
export default class BaseModel extends Model {
  static modelPaths = [__dirname];
  static pickJsonSchemaProperties = true;
  createdAt!: Date;
  updatedAt!: Date;
  deletedAt!: Date | null;
  deletedReason!: string | null;
  updatedBy!: string | null;
  deletedBy!: string | null;
  clientSource!: string | null;

  $beforeInsert() {
    this.createdAt = this.createdAt ? new Date(this.createdAt) : new Date();
    this.updatedAt = new Date();
  }

  $beforeUpdate() {
    this.updatedAt = new Date();
  }
}
