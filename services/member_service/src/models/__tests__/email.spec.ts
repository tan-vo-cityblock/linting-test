import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { Email, IEmail, IEmailQueryAttributes } from '../email';

describe('Email model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => { txn = await transaction.start(Model.knex()); });

  afterEach(async () => txn.rollback());

  describe('Email.create', () => {
    it('creates an email', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const expectedEmails = ['bobby@gmail.com', 'fischer@gmail.com'];
      const emails: IEmail[] = [
        { email: expectedEmails[0] },
        { email: expectedEmails[1] },
      ];
  
      const result: Email[] = await Email.create(memberId, emails, txn);
      result.forEach((email) => {
        expect(email.id).toBeDefined();
        expect(expectedEmails).toContain(email.email);
      });
    });
  
    it('returns null when passed empty email objects', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const emails: IEmail[] = [{ email: null }];
  
      const result: Email[] = await Email.create(memberId, emails, txn);
      expect(result).toBeNull();
    });
  
    it('returns null when passed an empty email array', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const emails: IEmail[] = [];
  
      const result: Email[] = await Email.create(memberId, emails, txn);
      expect(result).toBeNull();
    });
  });

  describe('Email.getAllByAttributes', () => {
    it('returns a matching email on the filter', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const emails: IEmail[] = [{ email: "danaerys@gmail.com" }];
      await Email.create(memberId, emails, txn);

      const emailFilter: IEmailQueryAttributes = { email: "danaerys@gmail.com" };
      const result: Email[] = await Email.getAllByAttributes(emailFilter, txn);
      expect(result[0].memberId).toBe(memberId);
      expect(result[0].email).toBe("danaerys@gmail.com");
    });

    it('returns a matching email on the filter with different casing', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const emails: IEmail[] = [{ email: "DROGON@WESTER.OS" }];
      await Email.create(memberId, emails, txn);

      const emailFilter: IEmailQueryAttributes = { email: "drogon@wester.os" };
      const result: Email[] = await Email.getAllByAttributes(emailFilter, txn);
      expect(result[0].memberId).toBe(memberId);
      expect(result[0].email).toBe("DROGON@WESTER.OS");
    });

    it('returns no matching email', async () => {
      const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
      const emails: IEmail[] = [{ email: "DROGON@WESTER.OS" }];
      await Email.create(memberId, emails, txn);

      const emailFilter: IEmailQueryAttributes = { email: "danaerys@gmail.com" };
      const result: Email[] = await Email.getAllByAttributes(emailFilter, txn);
      expect(result).toHaveLength(0);
    });
  });
});
