import { Request, Response } from 'express';
import { knex } from '../db';

const refreshHieMessageSignal = async (req: Request, res: Response) => {
  try {
    await knex.raw(`REFRESH MATERIALIZED VIEW CONCURRENTLY hie_message_signal;`);
  } catch (err) {
    console.error('Error in refresh-hie-message-signal.ts: ', err);
    res.status(400).send('Error refreshing view.');
    return;
  }
  res.status(204).send();
  return;
};

export default refreshHieMessageSignal;
