import { eventTracker } from "$lib/db";
import { exit as workerExit, start } from "$lib/worker_manager.js";

const exit = () => {
  workerExit();
  eventTracker.exit();
};
start();

process.on("SIGINT", exit);
process.on("SIGTERM", exit);
