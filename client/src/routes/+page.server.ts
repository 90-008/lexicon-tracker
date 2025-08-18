import { fetchEvents, fetchTrackingSince } from "$lib/api";

export const load = async () => {
  const events = await fetchEvents();
  const trackingSince = await fetchTrackingSince();
  return { events, trackingSince };
};
