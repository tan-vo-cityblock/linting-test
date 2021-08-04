import { Request, Response } from 'express';
import { isEmpty } from 'lodash';
import { HieMessage } from '../models';

const createHieMessage = async (req: Request, res: Response) => {
  if (!req.body || isEmpty(req.body)) {
    const message = 'No Pub/Sub message received.';
    res.status(400).send(`Bad Request: ${message}`);
    return;
  }

  try {
    const message = Buffer.from(req.body.message.data, 'base64').toString();
    const parsedMessage = JSON.parse(message);
    const existingHieMessage = await HieMessage.findByMessageId(parsedMessage.messageId);
    if (!existingHieMessage) {
      await HieMessage.create({
        eventType: parsedMessage.eventType,
        messageId: parsedMessage.messageId,
        patientId: parsedMessage.patientId,
        payload: parsedMessage.payload,
      });
    }
  } catch (err) {
    console.error('Error in create-hie-message.ts: ', err);
    res.status(400).send('Bad Request: Please check your request body.');
    return;
  }

  res.status(204).send();
  return;
};

export default createHieMessage;
