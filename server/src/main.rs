use std::sync::Arc;

use atproto_jetstream::{CancellationToken, Consumer, EventHandler, JetstreamEvent};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    api::serve,
    db::{Db, EventRecord},
};

mod api;
mod db;
mod error;

const BSKY_ZSTD_DICT: &[u8] = include_bytes!("./bsky_zstd_dictionary");

struct JetstreamHandler {
    tx: Sender<EventRecord>,
}

impl JetstreamHandler {
    fn new() -> (Self, Receiver<EventRecord>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        (Self { tx }, rx)
    }
}

#[async_trait::async_trait]
impl EventHandler for JetstreamHandler {
    async fn handle_event(&self, event: JetstreamEvent) -> anyhow::Result<()> {
        if let Some(e) = EventRecord::from_jetstream(event) {
            self.tx.send(e).await?;
        }
        Ok(())
    }

    fn handler_id(&self) -> String {
        "handler".to_string()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt().compact().init();

    let db = Arc::new(Db::new().expect("couldnt create db"));

    tokio::fs::write("./bsky_zstd_dictionary", BSKY_ZSTD_DICT)
        .await
        .expect("could not write bsky zstd dict");

    let jetstream = Consumer::new(atproto_jetstream::ConsumerTaskConfig {
        compression: true,
        jetstream_hostname: "jetstream2.us-west.bsky.network".into(),
        collections: Vec::new(),
        dids: Vec::new(),
        max_message_size_bytes: None,
        cursor: None,
        require_hello: true,
        zstd_dictionary_location: "./bsky_zstd_dictionary".into(),
        user_agent: "nsid-tracker/0.0.1".into(),
    });

    let (event_handler, mut event_rx) = JetstreamHandler::new();

    let cancel_token = CancellationToken::new();
    tokio::spawn(async move {
        jetstream
            .register_handler(Arc::new(event_handler))
            .await
            .expect("cant register handler");
        jetstream
            .run_background(cancel_token.clone())
            .await
            .expect("cant run jetstream");
    });

    std::thread::spawn({
        let db = db.clone();
        move || {
            while let Some(e) = event_rx.blocking_recv() {
                if let Err(e) = db.record_event(e) {
                    tracing::error!("failed to record event: {}", e);
                }
            }
        }
    });

    serve(db).await;
}
