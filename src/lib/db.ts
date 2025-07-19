import { Database } from "bun:sqlite";
import { env } from "process";
import type { WorkerEventData } from "./types";

export interface EventRecord {
  nsid: string;
  timestamp: number;
  count: number;
  deleted_count: number;
}

const ALL_NSID = "*";

class EventTracker {
  private db: Database;
  private insertNsidQuery;
  private insertEventQuery;
  private updateCountQuery;
  private getNsidCountQuery;

  // private bufferedRecords: WorkerEventData[];

  constructor() {
    // this.bufferedRecords = [];
    this.db = new Database(env.DB_PATH ?? "events.sqlite");
    // init db
    this.db.run("PRAGMA journal_mode = WAL;");
    // events
    this.db.run(`
      CREATE TABLE IF NOT EXISTS events (
        nsid_idx INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        deleted INTEGER NOT NULL,
        PRIMARY KEY (nsid_idx, timestamp)
      )
    `);
    // aggregated counts
    this.db.run(`
      CREATE TABLE IF NOT EXISTS nsid_counts (
        nsid_idx INTEGER PRIMARY KEY,
        count INTEGER NOT NULL,
        deleted_count INTEGER NOT NULL,
        last_updated INTEGER NOT NULL
      )
    `);
    this.db.run(`
      CREATE TABLE IF NOT EXISTS nsid_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nsid TEXT UNIQUE NOT NULL
      );
    `);
    // compile queries
    this.insertNsidQuery = this.db.query(
      "INSERT OR IGNORE INTO nsid_types (nsid) VALUES (?)",
    );
    this.insertEventQuery = this.db.query(`
      INSERT OR IGNORE INTO events (nsid_idx, timestamp, deleted)
      VALUES (
        (SELECT id FROM nsid_types WHERE nsid = ?),
        ?,
        ?
      )
    `);
    this.updateCountQuery = this.db.query(`
      INSERT INTO nsid_counts (nsid_idx, count, deleted_count, last_updated)
      VALUES (
        (SELECT id FROM nsid_types WHERE nsid = $nsid),
        1 - $deleted,
        $deleted,
        $timestamp
      )
      ON CONFLICT(nsid_idx) DO UPDATE SET
        count = count + (1 - $deleted),
        deleted_count = deleted_count + $deleted,
        last_updated = $timestamp
    `);
    this.getNsidCountQuery = this.db.query(`
      SELECT
          (SELECT nsid FROM nsid_types WHERE id = nsid_idx) as nsid,
          count,
          deleted_count,
          last_updated as timestamp
      FROM nsid_counts
      ORDER BY count DESC
    `);
    this.insertNsidQuery.run(ALL_NSID);
  }

  // private recordBufferedEvents = () => {
  //   try {
  //     this.db.transaction(() => {
  //       for (const event of this.bufferedRecords) {
  //         this.insertEventQuery.run(event.nsid, event.timestamp, event.deleted);
  //       }
  //     })();
  //     this.bufferedRecords = [];
  //   } catch (e) {
  //     console.error(`can't commit to db: ${e}`);
  //   }
  // };

  recordEventHit = (data: WorkerEventData) => {
    try {
      this.db.transaction(() => {
        const { nsid, timestamp, deleted } = data;
        this.insertNsidQuery.run(nsid);
        this.insertEventQuery.run(nsid, timestamp, deleted);
        this.updateCountQuery.run({
          $nsid: nsid,
          $deleted: deleted,
          $timestamp: timestamp,
        });
        this.updateCountQuery.run({
          $nsid: ALL_NSID,
          $deleted: deleted,
          $timestamp: timestamp,
        });
      })();
      // this.bufferedRecords.push(data);
    } catch (e) {
      console.error(`can't commit to db: ${e}`);
    }
    // commit buffered if at 10k
    // if (this.bufferedRecords.length > 9999) this.recordBufferedEvents();
  };

  getNsidCounts = (): EventRecord[] => {
    return this.getNsidCountQuery.all() as EventRecord[];
  };

  exit = () => {
    // this.recordBufferedEvents();
    this.db.close();
  };
}

export const eventTracker = new EventTracker();
