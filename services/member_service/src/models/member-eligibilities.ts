import { Model } from 'objection';

interface IUpdateArgs {
  commonsHarpStatus: string;
}

export class MemberEligibilities {
  static query() {
    return Model.knex()('member_eligibilities');
  }

  static async getForMember(memberId: string) {
    return this.query().where({ memberId }).first();
  }

  static async upsertForMember(memberId: string, args: IUpdateArgs) {
    const eligibility = await this.getForMember(memberId);
    if (eligibility) await this.query().where({ memberId }).update(args);
    else await this.query().insert({ memberId, ...args });
    return this.getForMember(memberId);
  }
}
