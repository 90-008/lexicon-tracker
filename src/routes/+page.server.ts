import { eventTracker } from "$lib/db";

export const load = async () => {
  const events = eventTracker.getNsidCounts();
  const totalEvents = eventTracker.getEventCount();

  return {
    events,
    totalEvents,
  };
};
