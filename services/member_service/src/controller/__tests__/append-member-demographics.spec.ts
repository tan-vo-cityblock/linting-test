import { find } from 'lodash';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import { API_VERSION } from '../../app';
import { Address } from '../../models/address';
import { Email } from '../../models/email';
import { Phone } from '../../models/phone';
import { appendMemberDemographics } from '../append-member-demographics';
import {
    appendAddressesDemographicsRequest,
    appendDemographicsRequest,
    appendEmailsDemographicsRequest,
    appendPhonesDemographicsRequest,
} from './test-data';
import { setupDb } from './test-setup';

describe('Append member demographics route', () => {
    let memberId: string;
    let txn: Transaction;
    let testDb: ReturnType<typeof setupDb>;
    
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
        const insertEmails = Email.create(memberId, emails, txn);
        const insertPhones = Phone.create(memberId, phones, txn);
        const insertAddresses = Address.create(memberId, addresses, txn);
        await Promise.all([insertAddresses, insertEmails, insertPhones]);
    });

    afterEach(async () => txn.rollback());

    it('should successfully append emails', async () => {
        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'POST',
            url: `/${API_VERSION}/members/${memberId}/demographics`,
            params: { memberId },
            body: appendEmailsDemographicsRequest
        });

        request.txn = txn;

        await appendMemberDemographics(request, response);

        const _memberId_ = response._getData().memberId;
        expect(response.statusCode).toBe(200);
        expect(_memberId_).toBe(memberId);

        const currentMemberEmails = await Email.getAllByMemberId(memberId, txn);
        expect(currentMemberEmails).toHaveLength(2);
        expect(find(currentMemberEmails, { email: 'tdawg@gmail.com' })).toBeTruthy();
    });

    it('should successfully append phones', async () => {
        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'POST',
            url: `/${API_VERSION}/members/${memberId}/demographics`,
            params: { memberId },
            body: appendPhonesDemographicsRequest
        });

        request.txn = txn;

        await appendMemberDemographics(request, response);

        const _memberId_ = response._getData().memberId;
        expect(response.statusCode).toBe(200);
        expect(_memberId_).toBe(memberId);

        const currentMemberPhones = await Phone.getAllByMemberId(memberId, txn);
        expect(currentMemberPhones).toHaveLength(2);
        expect(find(currentMemberPhones, { phone: '6159530693' })).toBeTruthy();
    });

    it('should successfully append addresses', async () => {
        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'POST',
            url: `/${API_VERSION}/members/${memberId}/demographics`,
            params: { memberId },
            body: appendAddressesDemographicsRequest
        });

        request.txn = txn;

        await appendMemberDemographics(request, response);

        const _memberId_ = response._getData().memberId;
        expect(response.statusCode).toBe(200);
        expect(_memberId_).toBe(memberId);

        const currentMemberAddresses = await Address.getAllByMemberId(memberId, txn);
        expect(currentMemberAddresses).toHaveLength(2);
        expect(find(currentMemberAddresses, {
            street1: '1 Main St',
            city: 'Boston',
            state: 'MA',
            zip: '02101'
        })).toBeTruthy();
    });

    it('should successfully a combination of emails/phones/addresses', async () => {
        const response = httpMocks.createResponse();
        const request = httpMocks.createRequest({
            method: 'POST',
            url: `/${API_VERSION}/members/${memberId}/demographics`,
            params: { memberId },
            body: appendDemographicsRequest
        });

        request.txn = txn;

        await appendMemberDemographics(request, response);

        const _memberId_ = response._getData().memberId;
        expect(response.statusCode).toBe(200);
        expect(_memberId_).toBe(memberId);

        const currentMemberEmails = await Email.getAllByMemberId(memberId, txn);
        const currentMemberAddresses = await Address.getAllByMemberId(memberId, txn);
        const currentMemberPhones = await Phone.getAllByMemberId(memberId, txn);

        expect(currentMemberPhones).toHaveLength(2);
        expect(currentMemberEmails).toHaveLength(2);
        expect(currentMemberAddresses).toHaveLength(2);

        expect(find(currentMemberPhones, { phone: '6159530693' })).toBeTruthy();
        expect(find(currentMemberEmails, { email: 'tdawg@gmail.com' })).toBeTruthy();
        expect(find(currentMemberAddresses, {
            street1: '1 Main St',
            city: 'Boston',
            state: 'MA',
            zip: '02101'
        })).toBeTruthy();
    });

});
