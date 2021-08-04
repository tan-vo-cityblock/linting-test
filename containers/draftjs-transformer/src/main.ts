import fs from 'fs';

import {
  ContentState,
  Modifier,
  SelectionState,
  convertFromRaw,
} from 'draft-js';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import isEmpty from 'lodash/isEmpty';

const { stateToMarkdown: originalStateToMarkdown } = require('draft-js-export-markdown');

const LOG_HEADER = '[[DRAFTJS-TRANSFORMER]]'
const ZERO_WIDTH_SPACE_PLACEHOLDER = '\n\uFEFF\n';
const ZERO_WIDTH_SPACE = '\u200B';

const stateToMarkdown = (state: ContentState) => {
  let newState = state;
  const isMention = (key: string) => state.getEntity(key).getData().mention;

  // Find blocks that contain mentions
  state.getBlocksAsArray().forEach((block) => {
    let replacements: Array<{ selectionState: SelectionState; id: string }> = [];
    block.findEntityRanges(
      (val) => val.getEntity() && isMention(val.getEntity()),
      (start, end) => {
        // Create a selection of the mention text
        const selectionState = SelectionState.createEmpty(block.getKey()).merge({
          anchorOffset: start,
          focusOffset: end,
        });

        // Replace that mention text with the @uuid syntax
        const id = state.getEntity(block.getEntityAt(start)).getData().mention.id;
        replacements = [...replacements, { selectionState, id }];
      },
    );
    replacements.reverse().forEach(({ selectionState, id }) => {
      newState = Modifier.replaceText(newState, selectionState, `@${id}`);
    });
  });

  // Convert to markdown removing zero-width-space placeholder hack
  return originalStateToMarkdown(newState)
    .split(ZERO_WIDTH_SPACE_PLACEHOLDER)
    .join(ZERO_WIDTH_SPACE);
};

const getTextFromContentState = (noteContent: string) => {
  const rawNoteContent = JSON.parse(noteContent);
  if (isEmpty(rawNoteContent)) return '';
  return stateToMarkdown(convertFromRaw(rawNoteContent)).replace(/\n$/, '');
};

async function main(
  gcpProject: string, 
  readDataset: string, 
  readTable: string,
  storageBucket: string,
  writeDataset: string,
  writeTable: string,
  ephermeralStorage: string
) {
    // Declaring reusable clients up front
    const bigquery = new BigQuery({projectId: gcpProject, location: 'US'});
    const storage = new Storage({projectId: gcpProject});
    const gcsDestination = `node/draftjs/${readTable}.json`;

    async function transformData() {
      const tableToTransform = bigquery
        .dataset(readDataset)
        .table(readTable);
      
      const [rows] = await tableToTransform.getRows();    
      console.log(`${LOG_HEADER} Begin transforming data for ${readTable}`);
      const transformedRows = rows.map(row => {
        delete row.text;
        return {
          ...row,
          createdAt: row.createdAt.value,
          updatedAt: row.updatedAt.value, 
          deletedAt: row.deletedAt?.value, 
          text: getTextFromContentState(row.contentState)
        }
      });
      return transformedRows;
    }
    async function writeToGcs(data) {
      const writePath = `${ephermeralStorage}/${readTable}.json`;
      const file = fs.createWriteStream(writePath);
      console.log(`${LOG_HEADER} Begin writing data to file ${writePath} ...`);
      data.forEach(row => file.write(JSON.stringify(row) + '\n'));
      file.end();
      console.log(`${LOG_HEADER} Write complete, uploading to ${storageBucket}/${gcsDestination} ...`);
      const bucket = storage.bucket(storageBucket);
      await bucket.upload(writePath, {destination: gcsDestination});
      console.log(`${LOG_HEADER} Uploaded transformed data to GCS to ${storageBucket}/${gcsDestination}`);
    }
    async function writeToBigQuery() {
      const tableToTransform = bigquery
        .dataset(readDataset)
        .table(readTable);
      const [metadata] = await tableToTransform.getMetadata();
      const new_schema = metadata.schema;
      const existingTextField = new_schema.fields.filter(field => {
        if (field.name === 'text') {
          return field
        }
      });
      if (existingTextField.length === 0) {
        const column = {name: 'text', type: 'STRING', mode: 'NULLABLE'};
        new_schema.fields.push(column);
        console.log(`${LOG_HEADER} Adding new column for transformed data`);
      }
      const loadMetadata = {
        createDisposition: 'CREATE_IF_NEEDED',
        writeDisposition: 'WRITE_TRUNCATE',
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        schema: new_schema,
        location: 'US',
      };
      const [job] = await bigquery
        .dataset(writeDataset)
        .table(writeTable)
        .load(storage.bucket(storageBucket).file(gcsDestination), loadMetadata);
    
      // Check the job's status for errors
      const errors = job.status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }
      console.log(`${LOG_HEADER} Completed writing data for Job ID ${job.id} into ${writeDataset}.${writeTable}`);
    }

    /**
     * Orchestration occurs here:
     * - read and transform data into Array of objects
     * - write this to GCS
     * - load from GCS into BigQuery
     */
    const transformedRows = await transformData();
    await writeToGcs(transformedRows);
    writeToBigQuery();
}

/**
 * Code execution starts here, get arguments then run the transfomer
 */
const args = process.argv.slice(3);  // --unhandled-rejections=strict is third arg
if (args.length != 7) {
  throw new Error('Draft.js Transformer requires 7 args to run!!!');
}
const [gcpProject, readDataset, readTable, storageBucket, writeDataset, writeTable, ephermeralStorage] = args;
main(gcpProject, readDataset, readTable, storageBucket, writeDataset, writeTable, ephermeralStorage);
