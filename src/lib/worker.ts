import { JetstreamSubscription } from "@atcute/jetstream";
import type { WorkerEventData, WorkerCommand } from "./types.js";
import * as wt from "node:worker_threads";

const port = wt.parentPort!;
let subscription: JetstreamSubscription | null = null;

const track = async () => {
  subscription = new JetstreamSubscription({
    url: "wss://jetstream2.us-east.bsky.network",
    validateEvents: false, // trust the jetstream :3
  });

  for await (const event of subscription) {
    if (event.kind !== "commit") {
      continue;
    }

    const { operation, collection } = event.commit;

    const data: WorkerEventData = {
      nsid: collection,
      timestamp: event.time_us,
      deleted: operation === "delete",
    };

    port.postMessage(data);
  }
};

const trackLoop = async () => {
  if (subscription !== null) {
    return;
  }

  let retryCount = 0;
  const baseDelay = 1000; // 1 second
  const maxDelay = 60000; // 60 seconds

  // if the above fails we fall into here
  while (true) {
    try {
      await track();
      retryCount = 0; // Reset on success
    } catch (e) {
      console.log(`tracking failed: ${e}`);

      const delay = Math.min(baseDelay * Math.pow(2, retryCount), maxDelay);
      console.log(`retrying in ${delay}ms (attempt ${retryCount + 1})`);

      await new Promise((resolve) => setTimeout(resolve, delay));
      retryCount++;
    }
  }
};

port.on("message", (command: WorkerCommand) => {
  if (command === "exit") process.exit();
});

trackLoop().catch(console.error);
