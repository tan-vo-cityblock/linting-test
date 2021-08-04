import { ICreateMemberRequest } from '../controller/types';
import { getOrCreateTransaction, IRequestWithTransaction } from '../controller/utils';
import { Address, IAddress } from '../models/address';
import { Email, IEmail } from '../models/email';
import { Member } from '../models/member';
import { MemberInsurance } from '../models/member-insurance';
import { MemberDemographics } from '../models/member-demographics';
import { IPhone, Phone } from '../models/phone';
import { deletePatientInElation } from '../util/elation';

// tslint:disable no-console
export async function createMemberInternal(request: IRequestWithTransaction, elationId: string) {
  const createMemberRequest: ICreateMemberRequest = request.body;
  const clientSource = request.get('clientSource');

  try {
    const createdMember = await getOrCreateTransaction(request.txn, Member.knex(), async (txn) => {
      const {
        categoryId,
        cohortId,
        insurances,
        partner,
        demographics,
        medicare,
        medicaid,
      }: ICreateMemberRequest = createMemberRequest;

      const newMember = await Member.create(
        partner,
        clientSource,
        cohortId,
        categoryId,
        elationId,
        txn,
      );

      const phones: IPhone[] = demographics.phones;
      const addresses: IAddress[] = demographics.addresses;
      const emails: IEmail[] = demographics.emails;

      await Promise.all([
        MemberDemographics.create(newMember.id, demographics, txn),
        Phone.create(newMember.id, phones, txn),
        Address.create(newMember.id, addresses, txn),
        Email.create(newMember.id, emails, txn),
      ]);

      const insurancePromises = [];

      insurancePromises.push(MemberInsurance.createInsurances(newMember.id, insurances, txn));

      if (!!medicare) {
        insurancePromises.push(
          MemberInsurance.createInsurances(newMember.id, medicare, txn),
        );
      }
      if (!!medicaid) {
        insurancePromises.push(
          MemberInsurance.createInsurances(newMember.id, medicaid, txn),
        );
      }
      await Promise.all(insurancePromises);

      console.log(`Created [member: ${newMember.id}, clientSource: ${clientSource}]`);
      return newMember;
    });
    return {
      patientId: createdMember.id,
      cityblockId: createdMember.cbhId,
      mrn: elationId || null,
    };
  } catch (e) {
    console.error('error in the controller create', e);
    if (!!elationId) {
      await deletePatientInElation(elationId);
    }
    return Promise.reject({ error: e.message });
  }
}
// tslint:enable no-console
