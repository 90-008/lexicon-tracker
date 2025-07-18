import { JetstreamSubscription } from "@atcute/jetstream";
import { eventTracker } from "./db.js";

let subscription: JetstreamSubscription | null = null;

export const startTracking = async () => {
  subscription = new JetstreamSubscription({
    url: "wss://jetstream2.us-east.bsky.network",
    // Don't filter by collections - we want to track all of them
  });

  for await (const event of subscription) {
    if (event.kind !== "commit") {
      continue;
    }

    const { operation, collection } = event.commit;

    eventTracker.addEvent(collection, event.time_us, operation === "delete");
  }
};
