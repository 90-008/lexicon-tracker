import { eventTracker } from "./db.js";
import type { WorkerCommand } from "./types.js";
import * as wt from "node:worker_threads";
import workerSrc from "./worker.js?raw";

let worker: wt.Worker | null = null;

const sendCommand = (command: WorkerCommand) => {
  worker?.postMessage(command);
};

export const start = () => {
  worker = new wt.Worker(workerSrc, { eval: true });
  worker.on("message", eventTracker.recordEventHit);
};

export const exit = () => {
  if (worker === null) return;
  sendCommand("exit");
  worker = null;
};
