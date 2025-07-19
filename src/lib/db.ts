import { Database } from "bun:sqlite";

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

  constructor() {
    this.db = new Database("events.sqlite");
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

  writeEvents = (
    events: { nsid: string; timestamp: number; deleted: boolean }[],
  ) => {
    this.db.transaction(() => {
      for (const event of events) {
        this.insertEventQuery.run(event.nsid, event.timestamp, event.deleted);
      }
    })();
  };

  recordEvent = (nsid: string, timestamp: number, deleted: boolean) => {
    this.db.transaction(() => {
      this.insertNsidQuery.run(nsid);
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
  };

  getNsidCounts = (): EventRecord[] => {
    return this.getNsidCountQuery.all() as EventRecord[];
  };

  close = () => {
    this.db.close();
  };
}

export const eventTracker = new EventTracker();
