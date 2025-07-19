import { eventTracker } from "./db.js";
import type { WorkerCommand } from "./types.js";
import * as wt from "node:worker_threads";

let worker: wt.Worker | null = null;

const sendCommand = (command: WorkerCommand) => {
  worker?.postMessage(command);
};

export const start = () => {
  worker = new wt.Worker("$lib/worker.js");
  worker.on("message", eventTracker.recordEventHit);
};

export const exit = () => {
  if (worker === null) return;
  sendCommand("exit");
  worker = null;
};
