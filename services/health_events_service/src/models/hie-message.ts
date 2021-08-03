import { knex } from '../db';

const TABLE = 'hie_message';

interface IHieMessage {
  id: string;
  createdAt: Date;
  eventType: string;
  messageId: string;
  patientId: string;
  payload: {};
}

type ICreateInput = Omit<IHieMessage, 'id' | 'createdAt'>;

const create = async (input: ICreateInput) => {
  const [response] = await knex<IHieMessage>(TABLE).insert(input).returning('*');
  return response;
};

const findByMessageId = async (messageId: string) =>
  knex<IHieMessage>(TABLE).where({ messageId }).first();

export default {
  create,
  findByMessageId,
};
