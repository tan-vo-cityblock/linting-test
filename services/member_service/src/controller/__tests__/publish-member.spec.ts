import { PubSub } from '@google-cloud/pubsub';
import { NextFunction } from 'express';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import sinon from 'sinon';
import { API_VERSION } from '../../app';
import { signPayload, stringifyMessage } from '../../middleware/publish-member-to-pubsub';
import { Member } from '../../models/member';
import { MemberDemographics } from '../../models/member-demographics';
import { updateAndPublishMember } from '../publish-member';
import {
  createAndPublishMemberRequest,
  memberPubSubMessage,
  partialPublishMemberRequest,
  partialMemberPubSubMessage,
} from './test-data';
import { setupDb } from './test-setup';

const sandbox = sinon.createSandbox();
describe('Publish member route', () => {
  const testTopicName = 'testTopicName';
  const testHmacSecret = 'JNLVwA7WXHHUfv4ZArPjRDJE';
  const testPubsub = new PubSub({});
  let publishStub;
  let topicStub;
  let txn: Transaction;
  let testDb: ReturnType<typeof setupDb>;

  beforeAll(async () => {
    testDb = setupDb();
  });

  afterAll(async () => testDb.destroy());

  beforeEach(async () => {
    jest.spyOn(console, 'error').mockImplementation(() => undefined);
    txn = await transaction.start(Model.knex());
  });

  afterEach(async () => txn.rollback());

  describe('on success', () => {
    beforeEach(async () => {
      publishStub = sandbox.stub().returns('testMessageId');
      topicStub = sandbox.stub(testPubsub, 'topic').returns({ publish: publishStub });
    });

    afterEach(async () => sandbox.restore());

    it('should successfully publish a member to pubsub', async () => {
      const member = await Member.get('a74bcb4a-d964-11e9-8372-acde48001122', txn);
      const memberId = member.id;
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'POST',
        url: `/${API_VERSION}/members/${memberId}/updateAndPublishMember`,
        params: { memberId },
        body: createAndPublishMemberRequest,
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line: no-console
      await updateAndPublishMember(request, response, console.log);

      const testData = stringifyMessage(memberPubSubMessage(memberId, member.cbhId, 'E1'));
      const msgAttributes = { hmac: signPayload(testData, testHmacSecret), topic: undefined };

      sandbox.assert.calledOnceWithExactly(topicStub, testTopicName);
      sandbox.assert.calledOnceWithExactly(publishStub, Buffer.from(testData), msgAttributes);

      const updatedMemberDemographics = await MemberDemographics.getByMemberId(memberId, txn);

      expect(updatedMemberDemographics.firstName).toBe('Perry');
      expect(updatedMemberDemographics.lastName).toBe('Platypus');
      expect(updatedMemberDemographics.dateOfBirth).toBe('1980-01-01');
      expect(updatedMemberDemographics.sex).toBe('male');

      expect(updatedMemberDemographics.phones).toEqual(
        expect.arrayContaining([expect.objectContaining({ phone: '213-234-1635' })]),
      );
      expect(updatedMemberDemographics.emails).toEqual(
        expect.arrayContaining([expect.objectContaining({ email: 'terry@gmail.com' })]),
      );
      expect(updatedMemberDemographics.addresses).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            street1: '1 Main Street',
            street2: 'Apartment 1',
            city: 'Winterfell',
            state: 'The North',
            zip: '11111',
          }),
        ]),
      );

      expect(response.statusCode).toBe(200);
    });

    it('should successfully publish a member to pubsub if partial demographic information is provided', async () => {
      const member = await Member.get('a74bcb4a-d964-11e9-8372-acde48001122', txn);
      const memberId = member.id;
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'POST',
        url: `/${API_VERSION}/members/${memberId}/updateAndPublishMember`,
        params: { memberId },
        body: partialPublishMemberRequest,
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line: no-console
      await updateAndPublishMember(request, response, console.log);

      const testData = stringifyMessage(partialMemberPubSubMessage(memberId, member.cbhId, 'E1'));
      const msgAttributes = { hmac: signPayload(testData, testHmacSecret), topic: undefined };

      sandbox.assert.calledOnceWithExactly(topicStub, testTopicName);
      sandbox.assert.calledOnceWithExactly(publishStub, Buffer.from(testData), msgAttributes);

      const updatedMemberDemographics = await MemberDemographics.getByMemberId(memberId, txn);

      expect(updatedMemberDemographics.lastName).toBe('Machine');
      expect(updatedMemberDemographics.race).toBe('Martian');
      expect(updatedMemberDemographics.language).toBe('platypan');
      expect(response.statusCode).toBe(200);
    });

    it('should successfully parse a json object into a json string for pubsub message requests', async () => {
      const member = await Member.get('a74bcb4a-d964-11e9-8372-acde48001122', txn);
      const memberId = member.id;
      const cbhId = member.cbhId;
      const mrn = 'E1';
      const pubsubMsg: string = stringifyMessage(
        partialMemberPubSubMessage(memberId, member.cbhId, mrn),
      );
      expect(pubsubMsg).toBe(
        `{"patientId":"${memberId}","cityblockId":"${cbhId}","mrn":"${mrn}","marketId":"13633ece-0356-493d-b3f5-5ee12fdb1892","partnerId":"e7db7bd3-327d-4139-b3cb-f376c6a32462","clinicId":"cf0da529-23c6-401c-8a2c-ab3add6beb9d","firstName":"Terry","lastName":"Machine","dob":"1970-01-01","race":"Martian","language":"platypan","maritalStatus":"currentlyMarried"}`,
      );
    });
  });

  describe('on failure', () => {
    beforeEach(async () => {
      publishStub = sandbox.stub().throws('TestError');
      topicStub = sandbox.stub(testPubsub, 'topic').returns({ publish: publishStub });
    });

    afterEach(async () => sandbox.restore());

    it('should fail to publish a member to pubsub if member does not exist', async () => {
      const memberId = '002da75e-90ac-4979-a997-b333f5ba5fb5';
      const next = jest.fn() as NextFunction;
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'POST',
        url: `/${API_VERSION}/members/${memberId}/updateAndPublishMember`,
        params: { memberId },
        body: createAndPublishMemberRequest,
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line:no-console
      await updateAndPublishMember(request, response, next);

      const expectedErrorMessage = `member does not exist on member attribution update [memberId: ${memberId}]`;
      expect(next).toHaveBeenCalledWith(new Error(expectedErrorMessage));
    });

    it('should fail to publish a member to pubsub if pubsub throws error', async () => {
      const memberId = 'a74bcb4a-d964-11e9-8372-acde48001122';

      const next = jest.fn() as NextFunction;
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'POST',
        url: `/${API_VERSION}/members/${memberId}/updateAndPublishMember`,
        params: { memberId },
        body: createAndPublishMemberRequest,
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line:no-console
      await updateAndPublishMember(request, response, next);

      sandbox.assert.threw(publishStub, 'TestError');
      expect(next).toHaveBeenCalledWith(expect.any(Error));
    });
  });
});
