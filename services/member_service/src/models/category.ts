import { Transaction } from 'objection';
import BaseModel from './base-model';

export const categoryNames = [
  'high cost',
  'high risk',
  'rising risk',
  'HARP',
  'removed',
  'lower risk'
];

export class Category extends BaseModel {
  static tableName = 'category';

  id!: number;
  name!: string;

  static async getById(id: number, txn: Transaction): Promise<Category> {
    return !!id ? this.query(txn).where({ id }).first() : null;
  }

  static async getByName(name: string, txn: Transaction): Promise<Category> {
    return !!name ? this.query(txn).where({ name }).first() : null;
  }

}
/* tslint:enable:member-ordering */
