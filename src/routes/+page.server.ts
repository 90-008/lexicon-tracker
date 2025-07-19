import { eventTracker } from "$lib/db";

export const load = async () => {
  const events = eventTracker.getNsidCounts();

  return {
    events,
  };
};
