import httpMocks from 'node-mocks-http';
import { createHieMessage } from '../';
import { knex } from '../../db';

describe('create-hie-message', () => {
  it('should return a 204 upon successful HIE Message creation', async () => {
    const payload = JSON.stringify({
      eventType: 'admit',
      messageId: 'random-id',
      patientId: 'patient-id',
      payload: {},
    });
    const request = httpMocks.createRequest({
      body: {
        message: {
          data: Buffer.from(payload).toString('base64'),
        },
      },
    });
    const response = httpMocks.createResponse();
    const send = jest.fn();
    response.status = jest.fn();
    (response.status as any).mockReturnValueOnce({ send });

    await createHieMessage(request, response);

    const hieEvents = await knex('hie_message').select('*');

    expect(hieEvents.length).toBe(1);
    expect(hieEvents[0].patientId).toBe('patient-id');
    expect(hieEvents[0].eventType).toBe('admit');
    expect(response.status).toBeCalledWith(204);
    expect(send).toBeCalled();
  });

  it('should return a 400 if the request doesnt include a body', async () => {
    const request = httpMocks.createRequest();
    const response = httpMocks.createResponse();
    const send = jest.fn();
    response.status = jest.fn();
    (response.status as any).mockReturnValueOnce({ send });

    await createHieMessage(request, response);

    const results = await knex('hie_message').select('*');

    expect(results.length).toBe(0);
    expect(response.status).toBeCalledWith(400);
    expect(send).toBeCalled();
  });
});
