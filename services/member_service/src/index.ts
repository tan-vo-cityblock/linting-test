import Knex from 'knex';
import { Model } from 'objection';
import app from './app';
import * as knexConfig from './models/knexfile';

// setting up knex configuration w/ objection
const config = (knexConfig as { [key: string]: any })[process.env.NODE_ENV || 'development'];
const knex = Knex(config);
Model.knex(knex);

const PORT = Number(process.env.PORT) || 8080;

// tslint:disable-next-line: no-console
app.listen(PORT, () => console.log(`App listening on port ${PORT}`));
