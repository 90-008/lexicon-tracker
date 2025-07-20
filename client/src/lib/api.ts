import { dev } from "$app/environment";
import type { EventRecord } from "./types";
import { PUBLIC_API_URL } from "$env/static/public";

export const fetchEvents = async (): Promise<EventRecord[]> => {
  const response = await fetch(
    `${dev ? "http" : "https"}://${PUBLIC_API_URL}/events`,
  );
  if (!response.ok) {
    throw new Error(`(${response.status}): ${await response.json()}`);
  }

  const data = await response.json();
  return data.events;
};
