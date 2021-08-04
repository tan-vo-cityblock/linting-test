import { isEmpty, isNil, omitBy } from 'lodash';
import { Model, RelationMappings, Transaction } from 'objection';
import BaseModel from './base-model';
import { Member } from './member';

export interface IEmail {
  id?: string;
  email: string;
}

export interface IEmailQueryAttributes {
  email?: string;
}

export class Email extends BaseModel {
  static tableName = 'email';

  id!: string;
  memberId!: string;
  email!: string;

  static get relationMappings(): RelationMappings {
    return {
      member: {
        relation: Model.BelongsToOneRelation,
        modelClass: Member,
        join: {
          from: 'email.memberId',
          to: 'member.id',
        },
      },
    };
  }

  static async create(memberId: string, emails: IEmail[], txn: Transaction) {
    const memberEmails = emails && emails
      .map((email) => omitBy(email, isNil))
      .filter((email) => !isEmpty(email))
      .map((email) => {
        return { ...email, memberId };
      });

    if (isEmpty(memberEmails)) {
      return null;
    }

    return this.query(txn).insertAndFetch(memberEmails);
  }

  static async createAllNew(memberId: string, emails: IEmail[], txn: Transaction): Promise<Email[]> {
    return Promise.all(
      emails.map(async (email: IEmail) => this.createIfNotExists(memberId, email, txn))
    );
  }

  static async createIfNotExists(memberId: string, email: IEmail, txn: Transaction): Promise<Email> {
    const emailQueryFilter: IEmailQueryAttributes = omitBy({ email: email.email }, isNil);
    const existingPhones = await this.getAllByAttributes(emailQueryFilter, txn);
    if (!isEmpty(existingPhones)) {
      return null;
    } else {
      return this.query(txn).insertAndFetch({ ...email, memberId });
    }
  }

  static async get(id: string, txn: Transaction) {
    return this.query(txn).findById(id);
  }

  static async getAllByAttributes(emailAttributes: IEmailQueryAttributes, txn: Transaction) {
    const { email } = emailAttributes;
    return this.query(txn).where(
      Email.knex().raw(`LOWER("email") = ?`, email.toLowerCase())
    );
  }

  static async getAllByMemberId(memberId: string, txn: Transaction) {
    return this.query(txn).where({ memberId, deletedAt: null });
  }

  static async delete(
    id: string,
    deletedReason: string | undefined,
    deletedBy: string,
    txn: Transaction
  ) {
    const email = await this.get(id, txn);
    if (!email) {
      throw new Error(`attempting to delete email that does not exist [id: ${id}]`);
    } else if (email.deletedAt) {
      return email;
    }

    const deletePatch = {
      deletedAt: new Date(),
      deletedReason: deletedReason || null,
      deletedBy
    };

    return this.query(txn)
      .where({ id, deletedAt: null })
      .patchAndFetchById(id, deletePatch);
  }

}
