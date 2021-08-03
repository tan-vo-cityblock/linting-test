import { buildFederatedSchema } from '@apollo/federation';
import { gql, ApolloServer } from 'apollo-server-express';
import express, { Request, Response } from 'express';
import fs from 'fs';
import path from 'path';
import * as controllers from './controllers';
import resolvers from './graphql/resolvers';

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// GraphQL
const schemaGql = fs.readFileSync(path.join(__dirname, './graphql/schema.graphql'), 'utf-8');
const typeDefs = gql`
  ${schemaGql}
`;
const server = new ApolloServer({
  schema: buildFederatedSchema([{ typeDefs, resolvers }]),
});
server.applyMiddleware({ app });

// Routes
app.get('/health-check', async (_, res: Response) => res.status(200).send(true));

// Consumers via Pub/Sub
app.post('/hie-messages', controllers.createHieMessage);

// Cloud Schedule/Cron
app.post('/schedule/refresh-hie-message-signal-view', controllers.refreshHieMessageSignal);

// Handles errors
app.use((err: Error & { status: number }, req: Request, res: Response) => {
  console.error('Found error on route', err);
  res.status(err.status || 500).send(err.message);
});

export default app;
