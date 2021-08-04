import bodyParser from 'body-parser';
import express, { NextFunction, Request, Response } from 'express';
import Knex from 'knex';
import { Model } from 'objection';
import { SourceNames } from './models/member-measure-source';

import { addMemberMeasures } from './controller/add-member-measures';
import { getElationMappedMeasures, getMeasures } from './controller/get-measures';
import { getMemberMeasures } from './controller/get-member-measures';
import { getMembersFromQuery } from './controller/get-members-from-query';

import * as knexConfig from './models/knexfile';
import { IExpressError } from './util/express-error';

const developmentEnv = 'development';
// setting up knex configuration w/ objection
const config = (knexConfig as { [key: string]: any })[process.env.NODE_ENV || developmentEnv];
const knex = Knex(config);
Model.knex(knex);

// setting up express w/ middleware
const PORT = Number(process.env.PORT) || 8080;
const API_KEY_COMMONS = process.env.API_KEY_COMMONS;
const API_KEY_ABLE = process.env.API_KEY_ABLE;
const API_KEY_ELATION = process.env.API_KEY_ELATION;
const app = express();

// bodyparser
app.use(bodyParser.json());

// API version
const API_VERSION = '1.0';

// authorization via api key in headers
if (!!API_KEY_COMMONS && !!API_KEY_ABLE) {
  app.use((req: Request, res: Response, next: NextFunction) => {
    if (!req.get('apiKey')) {
      return res.status(400).json({ error: 'Missing `apiKey` in header' });
    }

    if (req.get('apiKey') === API_KEY_COMMONS) {
      res.locals.sourceName = SourceNames.commons;
    } else if (req.get('apiKey') === API_KEY_ABLE) {
      res.locals.sourceName = SourceNames.able;
    } else if (req.get('apiKey') === API_KEY_ELATION) {
      res.locals.sourceName = SourceNames.elation;
    } else {
      return res.status(403).json({ error: `apiKey is incorrect` });
    }
    next();
  });
} else if (process.env.NODE_ENV === developmentEnv) {
  app.use((req: Request, res: Response, next: NextFunction) => {
    if (req.get('apiKey') === SourceNames.commons) {
      res.locals.sourceName = SourceNames.commons;
    } else if (req.get('apiKey') === SourceNames.able) {
      res.locals.sourceName = SourceNames.able;
    } else if (req.get('apiKey') === SourceNames.elation) {
      res.locals.sourceName = SourceNames.elation;
    } else {
      return res.status(403).json({ error: `dev apiKey is incorrect` });
    }
    next();
  });
} else {
  throw new Error(`Application missing API_KEY_[SOURCE]s env variable.`);
}

// config endpoints
app.get(`/${API_VERSION}/member/:memberId/measures`, getMemberMeasures);
app.get(`/${API_VERSION}/measures`, getMeasures);
app.get(`/${API_VERSION}/elationmap`, getElationMappedMeasures);
// had to make 'getMembersFromQuery' a post endpoint b/c axios get requests from Commons can't include a body and
// attempting to put a high number of memberIds in query params makes URL too long and results in errors.
app.post(`/${API_VERSION}/members`, getMembersFromQuery);
app.post(`/${API_VERSION}/member/:memberId/measures`, addMemberMeasures);

// handles errors
app.use((err: IExpressError, __: Request, res: Response, _: NextFunction) => {
  console.error('got error on route', err);
  res.status(err.status || 500).send(err.message);
});

app.listen(PORT, () => {
  // tslint:disable-next-line: no-console
  console.log(`App listening on port ${PORT}`);
});
