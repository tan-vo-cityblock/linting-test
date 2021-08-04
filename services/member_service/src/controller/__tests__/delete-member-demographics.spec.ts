import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { v4 as uuid } from 'uuid';
import { API_VERSION } from '../../app';
import { Address, IAddress } from '../../models/address';
import { Email, IEmail } from '../../models/email';
import { Member } from '../../models/member';
import { IPhone, Phone } from '../../models/phone';
import {
    deleteAddress,
    deleteEmail,
    deletePhone
} from '../delete-member-demographics';
import { setupDb } from './test-setup';

describe('Delete member demographics routes', () => {
    const userId: string = uuid();
    let memberId: string;
    let txn: Transaction;
    let testDb: ReturnType<typeof setupDb>;

    let insertedAddresses: IAddress[];
    let insertedPhones: IPhone[];
    let insertedEmails: IEmail[];
    
    beforeAll(async () => { testDb = setupDb() });
    
    afterAll(async () => testDb.destroy());

    beforeEach(async () => {
        // tslint:disable-next-line: no-empty
        jest.spyOn(console, 'error').mockImplementation(() => {});

        txn = await transaction.start(Model.knex());
        
        memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';
        
        const emails = [{ email: 'terry@mgmail.com' }];
        const phones = [{ phone: '2139456798', phoneType: 'home' }];
        const addresses = [{ street1: '1 Main St', city: 'New York', state: 'NY', zip: '10009' }];
        insertedEmails = await Email.create(memberId, emails, txn);
        insertedPhones = await Phone.create(memberId, phones, txn);
        insertedAddresses = await Address.create(memberId, addresses, txn);
    });

    afterEach(async () => txn.rollback());

    it('should successfully delete an address', async () => {
        const id = insertedAddresses[0].id;

        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'DELETE',
            url: `/${API_VERSION}/members/${memberId}/demographics/addresses/${id}`,
            params: { id, memberId },
            query: {
                deletedReason: 'some reason or another',
                deletedBy: userId,
            }
        });

        request.txn = txn;

        await deleteAddress(request, response);

        expect(response.statusCode).toBe(200);
        expect(response._getData().deleted).toBeTruthy();

        const currentMemberAddresses = await Address.getAllByMemberId(memberId, txn);
        expect(currentMemberAddresses).toHaveLength(0);

        const updatedAddress = await Address.get(id, txn);
        expect(updatedAddress).toMatchObject({
            deletedReason: 'some reason or another',
            deletedBy: userId,
        });
    });

    it('should successfully delete an email', async () => {
        const id = insertedEmails[0].id;

        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'DELETE',
            url: `/${API_VERSION}/members/${memberId}/demographics/emails/${id}`,
            params: { id, memberId },
            query: {
                deletedReason: 'some reason or another part 2',
                deletedBy: userId,
            }
        });

        request.txn = txn;

        await deleteEmail(request, response);

        expect(response.statusCode).toBe(200);
        expect(response._getData().deleted).toBeTruthy();

        const currentMemberEmails = await Email.getAllByMemberId(memberId, txn);
        expect(currentMemberEmails).toHaveLength(0);

        const updatedEmail = await Email.get(id, txn);
        expect(updatedEmail).toMatchObject({
            deletedReason: 'some reason or another part 2',
            deletedBy: userId,
        });
    });

    it('should successfully delete a phone', async () => {
        const id = insertedPhones[0].id;

        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'DELETE',
            url: `/${API_VERSION}/members/${memberId}/demographics/phones/${id}`,
            params: { id, memberId },
            query: {
                deletedReason: 'some reason or another part 2',
                deletedBy: userId,
            }
        });

        request.txn = txn;

        await deletePhone(request, response);

        expect(response.statusCode).toBe(200);
        expect(response._getData().deleted).toBeTruthy();

        const currentMemberPhones = await Phone.getAllByMemberId(memberId, txn);
        expect(currentMemberPhones).toHaveLength(0);

        const updatedPhone = await Phone.get(id, txn);
        expect(updatedPhone).toMatchObject({
            deletedReason: 'some reason or another part 2',
            deletedBy: userId,
        });
    });

    it('should prevent deletion if record is not associated with member', async () => {
        const anotherMember = await Member.create('emblem', null, null, null, null, txn);
        const anotherAddress = [{ street1: '2 Main St', city: 'Boston', state: 'MA', zip: '02112' }];
        const anotherInsertAddress = await Address.create(anotherMember.id, anotherAddress, txn);
        const { id } = anotherInsertAddress[0];

        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'DELETE',
            url: `/${API_VERSION}/members/${memberId}/demographics/phones/${id}`,
            params: { id, memberId },
            query: {
                deletedReason: 'some reason or another part 2',
                deletedBy: userId,
            }
        });

        request.txn = txn;

        await deleteAddress(request, response);

        expect(response.statusCode).toBe(400);
    });

});
