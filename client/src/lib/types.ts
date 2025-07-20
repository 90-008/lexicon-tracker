export type EventRecord = {
  nsid: string;
  last_seen: number;
  count: number;
  deleted_count: number;
};

export type NsidCounts = {
  last_seen: number;
  count: number;
  deleted_count: number;
};
