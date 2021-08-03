import { PubSub } from '@google-cloud/pubsub';
import { NextFunction } from 'express';
import httpMocks from 'node-mocks-http';
import { transaction, Model, Transaction } from 'objection';
import sinon from 'sinon';
import { API_VERSION } from '../../app';
import { signPayload, stringifyMessage } from '../../middleware/publish-member-to-pubsub';
import { resyncMember } from '../resync-member';
import { resyncPubSubMessage } from './test-data';
import { setupDb } from './test-setup';

const sandbox = sinon.createSandbox();
describe('Resync member route', () => {
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

    it('should resync member', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'GET',
        url: `/${API_VERSION}/members/resync/${memberId}`,
        params: { memberId }
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line: no-console
      await resyncMember(request, response, console.log);

      const testData = stringifyMessage(resyncPubSubMessage);
      const msgAttributes = { hmac: signPayload(testData, testHmacSecret), topic: undefined };

      sandbox.assert.calledOnceWithExactly(topicStub, testTopicName);
      sandbox.assert.calledOnceWithExactly(publishStub, Buffer.from(testData), msgAttributes);

      expect(response.statusCode).toBe(200);
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
        method: 'GET',
        url: `/${API_VERSION}/members/resync/${memberId}`,
        params: { memberId },
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line:no-console
      await resyncMember(request, response, next);

      expect(next).toHaveBeenCalledWith({
        error: expect.arrayContaining([`Member does not exist [memberId: ${memberId}]`])
      });
    });

    it('should fail to publish a member to pubsub if pubsub throws error', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const next = jest.fn() as NextFunction;
      const response = httpMocks.createResponse();
      const request = httpMocks.createRequest({
        method: 'GET',
        url: `/${API_VERSION}/members/resync/${memberId}`,
        params: { memberId },
      });

      request.hmacSecret = testHmacSecret;
      request.topicName = testTopicName;
      request.pubsub = testPubsub;
      request.txn = txn;

      // tslint:disable-next-line:no-console
      await resyncMember(request, response, next);

      sandbox.assert.threw(publishStub, 'TestError');
      expect(next).toHaveBeenCalledWith(expect.any(Error));
    });
  });
});
