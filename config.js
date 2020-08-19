const INGEST_INTERVAL = process.env.INGEST_INTERVAL || -1;
const SYNC_BASE_URL = process.env.SYNC_BASE_URL || 'https://mandaten.lblod.info';
const SYNC_FILES_PATH = process.env.SYNC_FILES_PATH || '/sync/mandatarissen/files';
const SYNC_FILES_ENDPOINT = `${SYNC_BASE_URL}${SYNC_FILES_PATH}`;
const DOWNLOAD_FILE_PATH = process.env.DOWNLOAD_FILE_PATH || '/files/:id/download';
const DOWNLOAD_FILE_ENDPOINT = `${SYNC_BASE_URL}${DOWNLOAD_FILE_PATH}`;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 100;

const PUBLIC_GRAPH = process.env.PUBLIC_GRAPH || 'http://mu.semte.ch/graphs/public';
const TMP_INGEST_GRAPH = process.env.TMP_INGEST_GRAPH || 'http://mu.semte.ch/graphs/tmp-ingest-gelinkt-notuleren-mandatarissen-consumer';

export {
  INGEST_INTERVAL,
  SYNC_BASE_URL,
  SYNC_FILES_ENDPOINT,
  DOWNLOAD_FILE_ENDPOINT,
  BATCH_SIZE,
  PUBLIC_GRAPH,
  TMP_INGEST_GRAPH
}
