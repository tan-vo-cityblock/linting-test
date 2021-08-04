import { Knex } from 'knex';
import { v4 as uuid } from 'uuid';
import seedData from './seed-data.json';

exports.seed = async (knex: Knex) => {
  await Promise.all(
    seedData.hie_messages.map(async (message) => {
      const hieMessage = {
        id: uuid(),
        patientId: message.patient.patientId,
        eventType: message.eventType,
        messageId: message.payload.Meta.Message.ID,
        eventDateTime: message.payload.Meta.EventDateTime,
        payload: message.payload,
      };
      await knex.table('hie_message').insert(hieMessage);
    }),
  );
};
