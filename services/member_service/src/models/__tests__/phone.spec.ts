import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { IPhone, Phone } from '../phone';

describe('Phone model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => { txn = await transaction.start(Model.knex())});

  afterEach(async () => txn.rollback());

  it('creates a phoneNumber', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const expectedPhones = ['3478762234', '9148765543'];
    const phones: IPhone[] = [
      {
        phone: expectedPhones[0],
      },
      {
        phone: expectedPhones[1],
      },
    ];

    const result: Phone[] = await Phone.create(memberId, phones, txn);
    result.forEach((phone) => {
      expect(phone.id).toBeDefined();
      expect(phone.phoneType).toEqual('main');
      expect(expectedPhones).toContain(phone.phone);
    });
  });

  it('returns null when passed empty phoneNumber objects', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const phones: IPhone[] = [
      {
        phone: null,
      },
    ];

    const result: Phone[] = await Phone.create(memberId, phones, txn);
    expect(result).toBeNull();
  });

  it('returns null when passed an empty phoneNumber array', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const phones: IPhone[] = [];

    const result: Phone[] = await Phone.create(memberId, phones, txn);
    expect(result).toBeNull();
  });
});
