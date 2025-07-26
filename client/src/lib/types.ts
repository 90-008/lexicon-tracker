export type Events = {
  per_second: number;
  events: Record<string, EventRecord>;
};
export type EventRecord = {
  last_seen: number;
  count: number;
  deleted_count: number;
};
export type NsidCount = {
  nsid: string;
  last_seen: number;
  count: number;
  deleted_count: number;
};
