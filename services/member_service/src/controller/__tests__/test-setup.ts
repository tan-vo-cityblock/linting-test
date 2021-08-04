import Knex from 'knex';
import Objection from 'objection';
import * as knexConfig from '../../models/knexfile';

export const setupDb = () => {
    const config = (knexConfig as Record<string, any>).test;
    const knex = Knex(config);
    Objection.Model.knex(knex);
    return {
        destroy: async () => {
            try {
                await knex.destroy();
            } catch (err) {
                console.error(err.message);
            }
        },
    };
};

test.skip('skip', () => undefined)