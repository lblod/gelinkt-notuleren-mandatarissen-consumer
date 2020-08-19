import requestPromise from 'request-promise';
import { app, errorHandler } from 'mu';
import { INGEST_INTERVAL } from './config';
import { getNextSyncTask, getRunningSyncTask, scheduleSyncTask, setTaskFailedStatus } from './lib/sync-task';
import { getUnconsumedFiles } from './lib/delta-file';
import { waitForDatabase } from './lib/database-utils';

/**
 * Core assumption of the microservice that must be respected at all times:
 *
 * 1. At any moment we know that the latest ext:deltaUntil timestamp
 *    on a task, either in failed/ongoing/success state, reflects
 *    the timestamp of the latest delta file that has been
 *    completly and successfully consumed
 * 2. Maximum 1 sync task is running at any moment in time
*/

async function triggerIngest() {
  if (INGEST_INTERVAL > 0) {
    console.log(`Executing scheduled function at ${new Date().toISOString()}`);
    requestPromise.post('http://localhost/ingest/');
    setTimeout( triggerIngest, INGEST_INTERVAL );
  }
}

waitForDatabase(async () => {
  const runningTask = await getRunningSyncTask();
  if (runningTask) {
    console.log(`Task <${runningTask.uri.value}> is still ongoing at startup. Updating its status to failed.`);
    await setTaskFailedStatus(runningTask.uri.value);
  }
  triggerIngest();
});

app.post('/ingest', async function( req, res, next ) {
  await scheduleSyncTask();

  const isRunning = await getRunningSyncTask();

  if (!isRunning) {
    const task = await getNextSyncTask();
    if (task) {
      console.log(`Start ingesting new delta files since ${task.since.toISOString()}`);
      try {
        const files = await getUnconsumedFiles(task.since);
        task.files = files;
        task.execute();
        return res.status(202).end();
      } catch(e) {
        console.log(`Something went wrong while ingesting. Closing sync task with failure state.`);
        console.trace(e);
        await task.closeWithFailure();
        return next(new Error(e));
      }
    } else {
      console.log(`No scheduled sync task found. Did the insertion of a new task just fail?`);
      return res.status(200).end();
    }
  } else {
    console.log('A sync task is already running. A new task is scheduled and will start when the previous task finishes.');
    return res.status(409).end();
  }
});

app.use(errorHandler);
