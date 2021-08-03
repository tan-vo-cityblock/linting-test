import { transaction, Model } from 'objection';
import { setupDb } from '../../lib/test-utils';
import { Address, IAddress } from '../address';

describe('Address model', () => {
  let testDb = null as any;
  let txn = null as any;

  beforeAll(async () => testDb = setupDb());

  afterAll(async () => testDb.destroy());

  beforeEach(async () => { txn = await transaction.start(Model.knex())});

  afterEach(async () => txn.rollback());

  it('creates an address', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const addresses: IAddress[] = [
      {
        street1: '36 Rugby road',
        street2: null,
        city: 'brooklyn',
        state: 'NY',
        zip: '11217',
      },
    ];

    const result: Address[] = await Address.create(memberId, addresses, txn);
    result.forEach((address) => {
      expect(address.id).toBeDefined();
      expect(address.street1).toEqual(addresses[0].street1);
      expect(address.street2).toEqual(addresses[0].street2);
      expect(address.city).toContain(addresses[0].city);
      expect(address.state).toContain(addresses[0].state);
      expect(address.zip).toContain(addresses[0].zip);
    });
  });

  it('returns null when passed empty address objects', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const addresses: IAddress[] = [
      {
        street1: null,
        street2: null,
        city: null,
        state: null,
        zip: null,
      },
    ];

    const result: Address[] = await Address.create(memberId, addresses, txn);
    expect(result).toBeNull();
  });

  it('should throw an exception, if required fields are null', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const addresses: IAddress[] = [
      {
        street1: '36 Rugby road',
        street2: null,
        city: 'brooklyn',
        state: null,
        zip: '11217',
      },
    ];

    try {
      expect.assertions(1);
      await Address.create(memberId, addresses, txn);
    } catch (e) {
      expect(e.message.split(' - ')[1]).toEqual(
        'null value in column "state" violates not-null constraint',
      );
    }
  });

  it('returns null when passed an empty address array', async () => {
    const memberId = 'a3ccab32-d960-11e9-b350-acde48001122';
    const addresses: IAddress[] = [];

    const result: Address[] = await Address.create(memberId, addresses, txn);
    expect(result).toBeNull();
  });
});
