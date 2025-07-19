import { JetstreamSubscription } from "@atcute/jetstream";
import { eventTracker } from "./db.js";

let subscription: JetstreamSubscription | null = null;
let eventsToCommit: {
  nsid: string;
  timestamp: number;
  deleted: boolean;
}[] = [];

export const writeEvents = () => {
  eventTracker.writeEvents(eventsToCommit);
  eventsToCommit = [];
};

const startTracking = async () => {
  subscription = new JetstreamSubscription({
    url: "wss://jetstream2.us-east.bsky.network",
    validateEvents: false, // trust the jetstream :3
  });

  for await (const event of subscription) {
    if (event.kind !== "commit") {
      continue;
    }

    const { operation, collection } = event.commit;

    eventTracker.recordEvent(collection, event.time_us, operation === "delete");
    eventsToCommit.push({
      nsid: collection,
      timestamp: event.time_us,
      deleted: operation === "delete",
    });

    if (eventsToCommit.length > 10000) {
      writeEvents();
    }
  }

  writeEvents();
};

export const track = async () => {
  let retryCount = 0;
  const baseDelay = 1000; // 1 second
  const maxDelay = 60000; // 60 seconds

  while (true) {
    try {
      await startTracking();
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
