import { omit } from 'lodash';
import { HieMessage } from '../';

const input = {
  eventType: 'admit',
  messageId: 'random-id',
  patientId: 'patient-id',
  payload: {
    foo: 'bar',
  },
};

describe('hie-message', () => {
  it('should create an HIE Message record', async () => {
    const result = await HieMessage.create(input);

    expect(result.id).toBeDefined();
    expect(result.createdAt).toBeDefined();

    const updatedResult = omit(result, ['id', 'createdAt']);
    expect(updatedResult).toStrictEqual(input);
  });
  it('finds an HIE Message record by messageId', async () => {
    await HieMessage.create(input);
    const result = await HieMessage.findByMessageId(input.messageId);

    expect(result).toBeDefined();
    expect(result?.createdAt).toBeDefined();
    const updatedResult = omit(result, ['id', 'createdAt']);
    expect(updatedResult).toStrictEqual(input);
  });
});
