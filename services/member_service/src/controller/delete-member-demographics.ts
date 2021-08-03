import { Response } from 'express';
import validate from 'uuid-validate';
import { Address } from '../models/address';
import { Email } from '../models/email';
import { Phone } from '../models/phone';
import { getOrCreateTransaction, IRequestWithTransaction } from './utils';

// tslint:disable no-console
export async function deleteAddress(
  request: IRequestWithTransaction,
  response: Response
) {
  try {
    const { id, memberId } = request.params;
    if (!validate(id)) {
      throw new Error(`attempted to delete invalid address id [id: ${id}]`);
    }

    const { deletedReason, deletedBy } = request.query;

    const address = await getOrCreateTransaction(
      request.txn,
      Address.knex(),
      async txn => {
        const memberAddresses = await Address.getAllByMemberId(memberId, txn);
        const memberAddressIds = memberAddresses.map(_address => _address.id);
        if (!memberAddressIds.includes(id)) {
          throw new Error(`cannot delete address not associated with member [memberId: ${memberId}, id: ${id}]`);
        }

        console.log(`deleting address [id: ${id}, deletedReason: ${deletedReason}, deletedBy: ${deletedBy}]`);
        return Address.delete(id, deletedReason, deletedBy, txn);
      }
    );

    response.send({
      deleted: true,
      deletedAt: address.deletedAt,
      deletedReason: address.deletedReason,
    });
  } catch (e) {
    console.error('error deleting address', e);
    response.status(400).send(e.message);
  }
}
// tslint:enable no-console

// tslint:disable no-console
export async function deleteEmail(
  request: IRequestWithTransaction,
  response: Response
) {
  try {
    const { id, memberId } = request.params;
    if (!validate(id)) {
      throw new Error(`attempted to delete invalid email id [id: ${id}]`);
    }

    const { deletedReason, deletedBy } = request.query;
    const email = await getOrCreateTransaction(
      request.txn,
      Email.knex(),
      async txn => {
        const memberEmails = await Email.getAllByMemberId(memberId, txn);
        const memberEmailIds = memberEmails.map(_email => _email.id);
        if (!memberEmailIds.includes(id)) {
          throw new Error(`cannot delete email not associated with member [memberId: ${memberId}, id: ${id}]`);
        }
        
        console.log(`deleting email [id: ${id}, deletedReason: ${deletedReason}, deletedBy: ${deletedBy}]`);
        return Email.delete(id, deletedReason, deletedBy, txn);
      }
    );

    response.send({
      deleted: true,
      deletedAt: email.deletedAt,
      deletedReason: email.deletedReason,
    });
  } catch (e) {
    console.error('error deleting email', e);
    response.status(400).send(e.message);
  }
}
// tslint:enable no-console

// tslint:disable no-console
export async function deletePhone(
  request: IRequestWithTransaction,
  response: Response
) {
  try {
    const { id, memberId } = request.params;
    if (!validate(id)) {
      throw new Error(`attempted to delete invalid phone id [id: ${id}]`);
    }

    const { deletedReason, deletedBy } = request.query;
    const phone = await getOrCreateTransaction(
      request.txn,
      Phone.knex(),
      async txn => {
        const memberPhones = await Phone.getAllByMemberId(memberId, txn);
        const memberPhoneIds = memberPhones.map(_phone => _phone.id);
        if (!memberPhoneIds.includes(id)) {
          throw new Error(`cannot delete phone not associated with member [memberId: ${memberId}, id: ${id}]`);
        }
        
        console.log(`deleting phone [id: ${id}, deletedReason: ${deletedReason}, deletedBy: ${deletedBy}]`);
        return Phone.delete(id, deletedReason, deletedBy, txn);
      }
    );

    response.send({
      deleted: true,
      deletedAt: phone.deletedAt,
      deletedReason: phone.deletedReason,
    });
  } catch (e) {
    console.error('error deleting phone', e);
    response.status(400).send(e.message);
  }
}
// tslint:enable no-console
