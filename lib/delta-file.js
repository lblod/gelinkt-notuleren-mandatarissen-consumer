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
    /** Id of the delta file */
    this.id = data.id;
    /** Creation datetime of the delta file */
    this.created = data.attributes.created;
    /** Name of the delta file */
    this.name = data.attributes.name;
  }

  /**
   * Public endpoint to download the delta file from based on its id
  */
  get downloadUrl() {
    return DOWNLOAD_FILE_ENDPOINT.replace(':id', this.id);
  }

  /**
   * Temporary location to store the delta file during processing
  */
  get tmpFilepath() {
    return `/tmp/${this.id}.json`;
  }

  /**
   * Trigger consumption of a delta file.
   * I.e. processing the insert/delete changesets and applying the changes
   * in the triple store taking into account the authorization rules.
   *
   * @param {function} onFinishCallback Callback executed when the processing of the delta file finished,
   *                         either successfully or unsuccessfully.
   *                         The callback function receives 2 arguments:
   *                         - the delta file object
   *                         - a boolean indicating success (true) or failure (false)
   * @method consume
   * @public
  */
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

  /**
   * Process the insert/delete changesets and apply the changes
   * in the triple store taking into account the authorization rules.
   *
   * @param {function} onFinishCallback Callback executed when the processing of the delta file finished,
   *                         either successfully or unsuccessfully.
   *                         The callback function receives 2 arguments:
   *                         - the delta file object
   *                         - a boolean indicating success (true) or failure (false)
   * @method ingest
   * @private
  */
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

/**
 * Get a list of produced delta files since a specific datetime
 *
 * @param {Date} since Datetime as of when to fetch delta files
 * @method getUnconsumedFiles
 * @public
*/
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

/**
 * Insert the list of triples in a temporary graph in the store
 *
 * @param {Array} triples Array of triples from an insert changeset
 * @method insertTriplesInTmpGraph
 * @private
*/
async function insertTriplesInTmpGraph(triples) {
  for (let i = 0; i < triples.length; i += BATCH_SIZE) {
    console.log(`Inserting ${triples.length} triples in temporary graph in batches. Current batch: ${i}-${i + BATCH_SIZE}`);
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

/**
 * Delete the triples from the given list from all graphs in the store, including the temporary graph.
 * Note: Triples are deleted one by one to avoid the need to use OPTIONAL in the WHERE clause
 *
 * @param {Array} triples Array of triples from an insert changeset
 * @method insertTriplesInTmpGraph
 * @private
*/
async function deleteTriplesInAllGraphs(triples) {
  console.log(`Deleting ${triples.length} triples one by one in all graphs`);
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

/**
 * Move triples from the temporary graph to the appropriate application graphs taking
 * the application's authorization rules into account.
 * Note: a change on the authorization rules of the application requires a revisit of this function
 *
 * @method moveTriplesFromTmpGraph
 * @private
*/
async function moveTriplesFromTmpGraph() {
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

/**
 * Transform an array of triples to a string of statements to use in a SPARQL query
 *
 * @param {Array} triples Array of triples to convert
 * @method toStatements
 * @private
*/
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
