import validate from 'uuid-validate';
import { getOrCreateTransaction, IRequestWithTransaction } from '../controller/utils';
import { Member } from '../models/member';

export async function memberValidation(
  request: IRequestWithTransaction,
  memberId: string
) {
  const errors = [];
  if (!validate(memberId)) {
    const failedRequirement = 'The memberId is not a valid UUID';
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errors.push(failedRequirement);
    return errors;
  }

  const member = await getOrCreateTransaction(request.txn, Member.knex(), async (txn) => {
    return Member.get(memberId, txn);
  });

  if (!member) {
    const failedRequirement = `Member does not exist [memberId: ${memberId}]`;
    console.error(`Unable to create insurance Id for member. ${failedRequirement}`);
    errors.push(failedRequirement);
  }

  return errors;
}
