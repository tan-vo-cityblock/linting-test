import Knex, { Knex as KnexType } from 'knex';
import config from '../config';
import * as knexConfig from '../models/knexfile';

const knexConfigForEnv: KnexType.Config = (knexConfig as Record<string, any>)[config.NODE_ENV];

export const knex = Knex(knexConfigForEnv);
