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

    if std::env::args()
        .nth(1)
        .map_or(false, |arg| arg == "migrate")
    {
        migrate_to_miniz();
        return;
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
                            let _ = event_tx.send(record).await;
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
            tracing::info!("starting ingest events thread...");
            while let Some(e) = event_rx.blocking_recv() {
                if let Err(e) = db.record_event(e) {
                    tracing::error!("failed to record event: {}", e);
                }
            }
        }
    });

    tokio::select! {
        res = serve(db, cancel_token.child_token()) => {
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
    ingest_events
        .join()
        .expect("failed to join ingest events thread");
}

fn migrate_to_miniz() {
    let from = Db::new(".fjall_data").expect("couldnt create db");
    let to = Db::new(".fjall_data_miniz").expect("couldnt create db");

    let mut total_count = 0_u64;
    for nsid in from.get_nsids() {
        tracing::info!("migrating {} ...", nsid.deref());
        for hit in from.get_hits(&nsid, ..).expect("cant read hits") {
            let (timestamp, data) = hit.expect("cant read event");
            to.record_event(EventRecord {
                nsid: nsid.to_smolstr(),
                timestamp,
                deleted: data.deleted,
            })
            .expect("cant record event");
            total_count += 1;
        }
    }

    tracing::info!("migrated {total_count} events!");
}
