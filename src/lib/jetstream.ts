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

export const startTracking = async () => {
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
