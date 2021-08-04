import { Response } from 'express';
import { Model } from 'objection';
import { getOrCreateTransaction, IRequestWithTransaction } from '../controller/utils';
import { memberValidation } from '../middleware/validate-member';
import { IMemberFilter, Member } from '../models/member';
import { createZenDeskUser, IZendeskRequest } from '../util/zendesk';

// tslint:disable no-console
// tslint:disable no-unnecessary-local-variable
export async function createZendeskMember(
  request: IRequestWithTransaction,
  response: Response,
) {  
  try {
    // first validate whether the requested member exists
    const { memberId } = request.params;
    const errors = await memberValidation(request, memberId);

    if (errors.length > 0) {
      return response.status(422).json({ error: errors });
    }  

    const body: IZendeskRequest = request.body;
    const responseBody = await getOrCreateTransaction(request.txn, Model.knex(), async (txn) => {
      const zenDeskUser = await createZenDeskUser(body, memberId, txn);
      return Member.update(memberId, { zendeskId: zenDeskUser.user.id } as IMemberFilter, txn);
    });

    response.send(responseBody);
  } catch (e) {
    console.log(`error in the controller get ${e}`);
    response.status(400).send(e.message);
  }
}
// tslint:enable no-console
