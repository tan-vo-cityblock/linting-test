import { PubSub } from '@google-cloud/pubsub';
import express from 'express';
import * as knex from 'knex';
import { transaction, Model, Transaction } from 'objection';

export interface IRequestWithPubSub extends express.Request {
  hmacSecret?: string;
  topicName?: string;
  pubsub?: PubSub;
  txn?: Transaction;
}

export interface IRequestWithTransaction extends express.Request {
  memberId?: string;
  txn?: Transaction;
}

export const getOrCreateTransaction = async <T>(
  explicitTransaction: Transaction | undefined,
  _knex_: knex | undefined,
  cb: (txn: Transaction) => Promise<T>,
): Promise<T> => {
  if (explicitTransaction) {
    return cb(explicitTransaction);
  } else if (knex) {
    return transaction(_knex_, cb);
  } else {
    return transaction(Model.knex(), cb);
  }
};
