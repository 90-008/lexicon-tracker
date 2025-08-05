use std::{ops::Deref, sync::Arc};

use smol_str::ToSmolStr;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::EnvFilter;

use crate::{
    api::serve,
    db::{Db, EventRecord},
    error::AppError,
    jetstream::JetstreamClient,
};

mod api;
mod db;
mod error;
mod jetstream;
mod utils;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
        Some("debug") => {
            debug();
            return;
        }
        Some(x) => {
            tracing::error!("unknown command: {}", x);
            return;
        }
        None => {}
    }

    let db = Arc::new(Db::new(".fjall_data").expect("couldnt create db"));

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("cant install rustls crypto provider");

    let mut jetstream =
        match JetstreamClient::new("wss://jetstream2.us-west.bsky.network/subscribe") {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("can't create jetstream client: {err}");
                return;
            }
        };

    let cancel_token = CancellationToken::new();

    let consume_events = tokio::spawn({
        let consume_cancel = cancel_token.child_token();
        let db = db.clone();
        async move {
            jetstream.connect().await?;
            loop {
                tokio::select! {
                    maybe_event = jetstream.read(consume_cancel.child_token()) => match maybe_event {
                        Ok(event) => {
                            let Some(record) = EventRecord::from_jetstream(event) else {
                                continue;
                            };
                            let db = db.clone();
                            tokio::task::spawn_blocking(move || {
                                if let Err(err) = db.record_event(record) {
                                    tracing::error!("failed to record event: {}", err);
                                }
                            });
                        }
                        Err(err) => return Err(err),
                    },
                    _ = consume_cancel.cancelled() => break Ok(()),
                }
            }
        }
    });

    std::thread::spawn({
        let db = db.clone();
        move || {
            loop {
                if db.is_shutting_down() {
                    break;
                }
                match db.sync(false) {
                    Ok(_) => (),
                    Err(e) => tracing::error!("failed to sync db: {}", e),
                }
                std::thread::sleep(std::time::Duration::from_secs(10));
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
    db.shutdown().expect("couldnt shutdown db");
}

fn debug() {
    let db = Db::new(".fjall_data").expect("couldnt create db");
    for nsid in db.get_nsids() {
        let nsid = nsid.deref();
        for hit in db.get_hits(nsid, ..) {
            let hit = hit.expect("cant read event");
            println!("{nsid} {}", hit.timestamp);
        }
    }
}

fn compact() {
    let from = Arc::new(Db::new(".fjall_data_from").expect("couldnt create db"));
    let to = Arc::new(Db::new(".fjall_data_to").expect("couldnt create db"));

    let mut threads = Vec::new();
    for nsid in from.get_nsids() {
        let from = from.clone();
        let to = to.clone();
        threads.push(std::thread::spawn(move || {
            tracing::info!("migrating {} ...", nsid.deref());
            let mut count = 0_u64;
            for hit in from.get_hits(&nsid, ..) {
                let hit = hit.expect("cant read event");
                let data = hit.access();
                to.record_event(EventRecord {
                    nsid: nsid.to_smolstr(),
                    timestamp: hit.timestamp,
                    deleted: data.deleted,
                })
                .expect("cant record event");
                count += 1;
            }
            count
        }));
    }

    let mut total_count = 0_u64;
    for thread in threads {
        total_count += thread.join().expect("thread panicked");
    }
    to.sync(true).expect("cant sync");
    tracing::info!("migrated {total_count} events!");
}
