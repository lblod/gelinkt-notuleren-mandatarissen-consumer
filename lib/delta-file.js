import requestPromise from 'request-promise';
import fs from 'fs-extra';
import request from 'request';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

import {
  PUBLIC_GRAPH,
  SYNC_BASE_URL,
  SYNC_FILES_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE
} from '../config';

class DeltaFile {
  constructor(data) {
    this.id = data.id;
    this.created = data.attributes.created;
    this.name = data.attributes.name;
  }

  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  get tmpFilepath() {
    return `/tmp/${this.id}.json`;
  }

  async consume(onFinishCallback) {
    const writeStream = fs.createWriteStream(this.tmpFilepath);
    writeStream.on('finish', () => this.ingest(onFinishCallback));

    try {
      request(this.downloadUrl)
        .on('error', function(err) {
          console.log(`Something went wrong while downloading file from ${this.downloadUrl}`);
          console.log(err);
          onFinishCallback(this, false);
        })
        .pipe(writeStream);
    } catch (e) {
      console.log(`Something went wrong while consuming the file ${this.id}`);
      await onFinishCallback(this, false);
    }

  }

  async ingest(onFinishCallback) {
    console.log(`Start ingesting file ${this.id} stored at ${this.tmpFilepath}`);
    try {
      const changeSets = await fs.readJson(this.tmpFilepath, { encoding: 'utf-8' });
      for (let { inserts, deletes } of changeSets) {
        // TODO insert/delete triples according to the authorization rules
        await insertTriples(inserts);
        await deleteTriples(deletes);
      }
      console.log(`Successfully finished ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      await onFinishCallback(this, true);
      await fs.unlink(this.tmpFilepath);
    } catch (e) {
      console.log(`Something went wrong while ingesting file ${this.id} stored at ${this.tmpFilepath}`);
      console.log(e);
      await onFinishCallback(this, false);
    }
  }
}

async function getUnconsumedFiles(since) {
  try {
    const result = await requestPromise({
      uri: SYNC_FILES_ENDPOINT,
      qs: {
        since: since.toISOString()
      },
      headers: {
        'Accept': 'application/vnd.api+json'
      },
      json: true // Automatically parses the JSON string in the response
    });
    return result.data.map(f => new DeltaFile(f));
  } catch (e) {
    console.log(`Unable to retrieve unconsumed files from ${SYNC_FILES_ENDPOINT}`);
    throw e;
  }
}


async function insertTriples(triples) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting triples in batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    await update(`
      INSERT DATA {
          GRAPH <${PUBLIC_GRAPH}> {
              ${statements}
          }
      }
    `);
  }
}

async function deleteTriples(triples) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Deleting triples in batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    await update(`
      DELETE DATA {
          GRAPH <${PUBLIC_GRAPH}> {
              ${statements}
          }
      }
    `);
  }
}

function toStatements(triples) {
  const escape = function(rdfTerm) {
    const { type, value, datatype, "xml:lang":lang } = rdfTerm;
    if (type == "uri") {
      return sparqlEscapeUri(value);
    } else if (type == "literal" || type == "typed-literal") {
      if (datatype)
        return `${sparqlEscapeString(value)}^^${sparqlEscapeUri(datatype)}`;
      else if (lang)
        return `${sparqlEscapeString(value)}@${lang}`;
      else
        return `${sparqlEscapeString(value)}`;
    } else
      console.log(`Don't know how to escape type ${type}. Will escape as a string.`);
      return sparqlEscapeString(value);
  };
  return triples.map(function(t) {
    const subject = escape(t.subject);
    const predicate = escape(t.predicate);
    const object = escape(t.object);
    return `${subject} ${predicate} ${object} . `;
  }).join('');
}

export {
  getUnconsumedFiles
}
