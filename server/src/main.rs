use std::{ops::Deref, time::Duration, u64, usize};

use itertools::Itertools;
use rclite::Arc;
use smol_str::ToSmolStr;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::EnvFilter;

use crate::{
    api::serve,
    db::{Db, DbConfig, EventRecord},
    error::AppError,
    jetstream::JetstreamClient,
    utils::{CLOCK, RelativeDateTime, get_time},
};

mod api;
mod db;
mod error;
mod jetstream;
mod utils;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .compact()
        .init();

    match std::env::args().nth(1).as_deref() {
        Some("compact") => {
            compact();
            return;
        }
        Some("migrate") => {
            migrate();
            return;
        }
        Some("debug") => {
            debug();
            return;
        }
        Some("print") => {
            print_all();
            return;
        }
        Some(x) => {
            tracing::error!("unknown command: {}", x);
            return;
        }
        None => {}
    }

    let cancel_token = CancellationToken::new();

    let db = Arc::new(
        Db::new(DbConfig::default(), cancel_token.child_token()).expect("couldnt create db"),
    );

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("cant install rustls crypto provider");

    let urls = [
        "wss://jetstream2.fr.hose.cam/subscribe",
        "wss://jetstream.fire.hose.cam/subscribe",
        "wss://jetstream1.us-west.bsky.network/subscribe",
        "wss://jetstream2.us-west.bsky.network/subscribe",
    ];
    let mut jetstream = match JetstreamClient::new(urls) {
        Ok(client) => client,
        Err(err) => {
            tracing::error!("can't create jetstream client: {err}");
            return;
        }
    };

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1000);
    let consume_events = tokio::spawn({
        let consume_cancel = cancel_token.child_token();
        async move {
            jetstream.connect().await?;
            loop {
                tokio::select! {
                    maybe_event = jetstream.read(consume_cancel.child_token()) => match maybe_event {
                        Ok(event) => {
                            let Some(record) = EventRecord::from_jetstream(event) else {
                                continue;
                            };
                            event_tx.send(record).await?;
                        }
                        Err(err) => return Err(err),
                    },
                    _ = consume_cancel.cancelled() => break Ok(()),
                }
            }
        }
    });

    let ingest_events = std::thread::spawn({
        let db = db.clone();
        move || {
            let mut buffer = Vec::new();
            loop {
                let read = event_rx.blocking_recv_many(&mut buffer, 500);
                if let Err(err) = db.ingest_events(buffer.drain(..)) {
                    tracing::error!("failed to ingest events: {}", err);
                }
                if read == 0 || db.is_shutting_down() {
                    break;
                }
            }
        }
    });

    let db_task = tokio::task::spawn({
        let db = db.clone();
        async move {
            let sync_period = Duration::from_secs(10);
            let mut sync_interval = tokio::time::interval(sync_period);
            sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let compact_period = std::time::Duration::from_secs(60 * 30); // 30 mins
            let mut compact_interval = tokio::time::interval(compact_period);
            compact_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                let sync_db = async || {
                    tokio::task::spawn_blocking({
                        let db = db.clone();
                        move || {
                            if db.is_shutting_down() {
                                return;
                            }
                            match db.sync(false) {
                                Ok(_) => (),
                                Err(e) => tracing::error!("failed to sync db: {}", e),
                            }
                        }
                    })
                    .await
                    .unwrap();
                };
                let compact_db = async || {
                    tokio::task::spawn_blocking({
                        let db = db.clone();
                        move || {
                            if db.is_shutting_down() {
                                return;
                            }
                            let end = get_time();
                            let start = end - compact_period;
                            let range = start.as_secs()..end.as_secs();
                            tracing::info!(
                                {
                                    start = %RelativeDateTime::from_now(start),
                                    end = %RelativeDateTime::from_now(end),
                                },
                                "running compaction...",
                            );
                            match db.compact_all(db.cfg.max_block_size, range, false) {
                                Ok(_) => (),
                                Err(e) => tracing::error!("failed to compact db: {}", e),
                            }
                        }
                    })
                    .await
                    .unwrap();
                };
                tokio::select! {
                    _ = sync_interval.tick() => sync_db().await,
                    _ = compact_interval.tick() => compact_db().await,
                    _ = db.shutting_down() => break,
                }
            }
        }
    });

    tokio::select! {
        res = serve(db.clone(), cancel_token.child_token()) => {
            if let Err(e) = res {
                tracing::error!("serve failed: {}", e);
            }
        }
        res = consume_events => {
            let err =
                res
                .map_err(AppError::from)
                .and_then(std::convert::identity)
                .expect_err("consume events cant return ok");
            tracing::error!("consume events failed: {}", err);
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received ctrl+c!");
            cancel_token.cancel();
        }
    }

    tracing::info!("shutting down...");
    cancel_token.cancel();
    ingest_events.join().expect("failed to join ingest events");
    db_task.await.expect("cant join db task");
    db.sync(true).expect("cant sync db");
}

fn print_all() {
    let db = Db::new(DbConfig::default(), CancellationToken::new()).expect("couldnt create db");
    let nsids = db.get_nsids().collect::<Vec<_>>();
    let mut count = 0_usize;
    for nsid in nsids {
        println!("{}:", nsid.deref());
        for hit in db.get_hits(&nsid, .., usize::MAX) {
            let hit = hit.expect("aaa");
            println!("{} {}", hit.timestamp, hit.deser().unwrap().deleted);
            count += 1;
        }
    }
    println!("total hits: {}", count);
}

fn debug() {
    let db = Db::new(DbConfig::default(), CancellationToken::new()).expect("couldnt create db");
    let info = db.info().expect("cant get db info");
    println!("disk size: {}", info.disk_size);
    for (nsid, blocks) in info.nsids {
        print!("{nsid}:");
        let mut last_size = 0;
        let mut same_size_count = 0;
        for item_count in blocks {
            if item_count == last_size {
                same_size_count += 1;
            } else {
                if same_size_count > 1 {
                    print!("x{}", same_size_count);
                }
                print!(" {item_count}");
                same_size_count = 0;
            }
            last_size = item_count;
        }
        print!("\n");
    }
}

fn compact() {
    let db = Db::new(
        DbConfig::default().ks(|c| {
            c.max_journaling_size(u64::MAX)
                .max_write_buffer_size(u64::MAX)
        }),
        CancellationToken::new(),
    )
    .expect("couldnt create db");
    let info = db.info().expect("cant get db info");
    db.major_compact().expect("cant compact");
    std::thread::sleep(Duration::from_secs(5));
    let compacted_info = db.info().expect("cant get db info");
    println!(
        "disk size: {} -> {}",
        info.disk_size, compacted_info.disk_size
    );
    for (nsid, blocks) in info.nsids {
        println!(
            "{nsid}: {} -> {}",
            blocks.len(),
            compacted_info.nsids[&nsid].len()
        )
    }
}

fn migrate() {
    let cancel_token = CancellationToken::new();
    let from = Arc::new(
        Db::new(
            DbConfig::default().path(".fjall_data_from"),
            cancel_token.child_token(),
        )
        .expect("couldnt create db"),
    );
    let to = Arc::new(
        Db::new(
            DbConfig::default().path(".fjall_data_to").ks(|c| {
                c.max_journaling_size(u64::MAX)
                    .max_write_buffer_size(u64::MAX)
                    .compaction_workers(rayon::current_num_threads() * 4)
                    .flush_workers(rayon::current_num_threads() * 4)
            }),
            cancel_token.child_token(),
        )
        .expect("couldnt create db"),
    );

    let nsids = from.get_nsids().collect::<Vec<_>>();
    let _eps_thread = std::thread::spawn({
        let to = to.clone();
        move || {
            loop {
                std::thread::sleep(Duration::from_secs(3));
                let eps = to.eps();
                if eps > 0 {
                    tracing::info!("{} rps", eps);
                }
            }
        }
    });
    let mut threads = Vec::with_capacity(nsids.len());
    let start = CLOCK.now();
    for nsid in nsids {
        let from = from.clone();
        let to = to.clone();
        threads.push(std::thread::spawn(move || {
            tracing::info!("{}: migrating...", nsid.deref());
            let mut count = 0_u64;
            for hits in from
                .get_hits(&nsid, .., usize::MAX)
                .chunks(100000)
                .into_iter()
            {
                to.ingest_events(hits.map(|hit| {
                    count += 1;
                    let hit = hit.expect("cant decode hit");
                    EventRecord {
                        nsid: nsid.to_smolstr(),
                        timestamp: hit.timestamp,
                        deleted: hit.deser().unwrap().deleted,
                    }
                }))
                .expect("cant record event");
            }
            tracing::info!("{}: ingested {} events...", nsid.deref(), count);
            count
        }));
    }
    let mut total_count = 0_u64;
    for thread in threads {
        let count = thread.join().expect("thread panicked");
        total_count += count;
    }
    let read_time = start.elapsed();
    let read_per_second = total_count as f64 / read_time.as_secs_f64();
    drop(from);
    tracing::info!("starting sync!!!");
    to.sync(true).expect("cant sync");
    tracing::info!("persisting...");
    let total_time = start.elapsed();
    let write_per_second = total_count as f64 / (total_time - read_time).as_secs_f64();
    tracing::info!(
        "migrated {total_count} events in {total_time:?} ({read_per_second:.2} rps, {write_per_second:.2} wps)"
    );
}
