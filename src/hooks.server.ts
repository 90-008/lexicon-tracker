import { eventTracker } from "$lib/db";
import { exit as workerExit, start } from "$lib/worker_manager.js";

const exit = () => {
  workerExit();
  eventTracker.exit();
  setTimeout(process.exit, 2000);
};
start();

process.on("SIGINT", exit);
process.on("SIGTERM", exit);
