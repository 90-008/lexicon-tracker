import { json } from "@sveltejs/kit";
import { eventTracker } from "$lib/db.js";

export const GET = async () => {
  try {
    const events = eventTracker.getNsidCounts();

    return json({ events });
  } catch (error) {
    console.error("error fetching events:", error);
    return json({ error: "failed to fetch events" }, { status: 500 });
  }
};
