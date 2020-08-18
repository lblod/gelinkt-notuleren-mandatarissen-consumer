import requestPromise from 'request-promise';
import fs from 'fs-extra';
import request from 'request';
import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapePredicate } from './utils';
import {
  SYNC_BASE_URL,
  SYNC_FILES_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE,
  PUBLIC_GRAPH,
  TMP_INGEST_GRAPH
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
        console.log(`Inserting data in temporary graph <${TMP_INGEST_GRAPH}>`);
        await insertTriplesInTmpGraph(inserts);
        console.log(`Deleting data in all graphs`);
        await deleteTriplesInAllGraphs(deletes);
      }
      console.log(`Moving data from temporary graph <${TMP_INGEST_GRAPH}> to relevant application graphs.`);
      await moveTriplesFromTmpGraph();
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


async function insertTriplesInTmpGraph(triples) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting triples in temporary graph in batch: ${i}-${i + BATCH_SIZE}`);
    const batch = triples.slice(i, i + BATCH_SIZE);
    const statements = toStatements(batch);
    await update(`
      INSERT DATA {
          GRAPH <${TMP_INGEST_GRAPH}> {
              ${statements}
          }
      }
    `);
  }
}

async function deleteTriplesInAllGraphs(triples) {
  console.log(`Deleting triples one by one in all graphs`);
  // triples are deleted one by one to avoid the need to use OPTIONAL in the WHERE block
  for (let i = 0; i < triples.length; i++) {
    const statements = toStatements([triples[i]]);
    await update(`
      DELETE WHERE {
          GRAPH ?g {
              ${statements}
          }
      }
    `);
  }
}

async function moveTriplesFromTmpGraph() {
  // TODO move data from tmp graph to other graphs
  // Move public data to the public graph
  const publicTypes = [
    'http://mu.semte.ch/vocabularies/ext/MandatarisStatusCode',
    'http://mu.semte.ch/vocabularies/ext/BeleidsdomeinCode',
    'http://data.vlaanderen.be/ns/mandaat#Mandataris',
    'http://www.w3.org/ns/org#Membership',
    'http://data.vlaanderen.be/ns/mandaat#Fractie',
    'http://data.vlaanderen.be/ns/mandaat#Mandaat',
    'http://mu.semte.ch/vocabularies/ext/BestuursfunctieCode',
    'http://data.vlaanderen.be/ns/besluit#Bestuursorgaan',
    'http://data.vlaanderen.be/ns/besluit#Bestuursorgaan',
    'http://mu.semte.ch/vocabularies/ext/BestuursorgaanClassificatieCode',
    'http://data.vlaanderen.be/ns/besluit#Bestuurseenheid',
    'http://mu.semte.ch/vocabularies/ext/BestuurseenheidClassificatieCode',
    'http://www.w3.org/ns/prov#Location'
  ];

  for (let type of publicTypes) {
    await update(`
    DELETE {
        GRAPH <${TMP_INGEST_GRAPH}> {
            ?s ?p ?o .
        }
    } INSERT {
        GRAPH <${PUBLIC_GRAPH}> {
            ?s ?p ?o .
        }
    } WHERE {
        GRAPH <${TMP_INGEST_GRAPH}> {
            ?s a <${type}> ; ?p ?o .
        }
    }`);
  }

  // Move organisational data to the correct organisation graph
  const organizationalTypes = [
    {
      type: 'http://www.w3.org/ns/person#Person',
      pathToBestuurseenheid: [
        "^http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
        "http://www.w3.org/ns/org#holds",
        "^http://www.w3.org/ns/org#hasPost",
        "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
        "http://data.vlaanderen.be/ns/besluit#bestuurt"
      ]
    },
    {
      type: 'http://data.vlaanderen.be/ns/persoon#Geboorte',
      pathToBestuurseenheid: [
        "^http://data.vlaanderen.be/ns/persoon#heeftGeboorte",
        "^http://data.vlaanderen.be/ns/mandaat#isBestuurlijkeAliasVan",
        "http://www.w3.org/ns/org#holds",
        "^http://www.w3.org/ns/org#hasPost",
        "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
        "http://data.vlaanderen.be/ns/besluit#bestuurt"
      ]
    }
  ];

  for (let config of organizationalTypes) {
    const predicatePath = config.pathToBestuurseenheid.map(p => sparqlEscapePredicate(p)).join('/');
    await update(`
    DELETE {
        GRAPH <${TMP_INGEST_GRAPH}> {
            ?s ?p ?o .
        }
    } INSERT {
        GRAPH ?organizationalGraph {
            ?s ?p ?o .
        }
    } WHERE {
        GRAPH <${TMP_INGEST_GRAPH}> {
            ?s a <${config.type}> ; ?p ?o .
        }
        ?s ${predicatePath} ?organization .
        GRAPH <${PUBLIC_GRAPH}> {
            ?organization <http://mu.semte.ch/vocabularies/core/uuid> ?organizationUuid .
        }
        BIND(IRI(CONCAT("http://mu.semte.ch/graphs/organizations/", ?organizationUuid)) as ?organizationalGraph)
    }`);
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
