import { knex } from '../db';

export const HIE_MESSAGE_SIGNAL_TABLE = 'hie_message_signal';

interface IHieMessageSignal {
  messageId: string;
  patientId: string;
  eventType: string;
  metaEventType: string;
  eventDateTime: Date;
  isReadmission: boolean;
}

export const find = async (messageId: string): Promise<IHieMessageSignal> => {
  const [response] = await knex(HIE_MESSAGE_SIGNAL_TABLE).where({ messageId }).returning('*');
  return response;
};

export default {
  find,
};
