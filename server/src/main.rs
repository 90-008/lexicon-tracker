use std::sync::Arc;

use atproto_jetstream::{CancellationToken, Consumer, EventHandler, JetstreamEvent};

use crate::{api::serve, db::Db};

mod api;
mod db;
mod error;

struct JetstreamHandler {
    db: Arc<Db>,
}

#[async_trait::async_trait]
impl EventHandler for JetstreamHandler {
    async fn handle_event(&self, event: JetstreamEvent) -> anyhow::Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let result = match event {
                JetstreamEvent::Commit {
                    time_us, commit, ..
                } => db.record_event(&commit.collection, time_us, false),
                JetstreamEvent::Delete {
                    time_us, commit, ..
                } => db.record_event(&commit.collection, time_us, true),
                _ => Ok(()),
            };
            if let Err(err) = result {
                tracing::error!("couldn't record event: {err}");
            }
        });
        Ok(())
    }

    fn handler_id(&self) -> String {
        "handler".to_string()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db = Arc::new(Db::new().expect("couldnt create db"));

    let consumer = Consumer::new(atproto_jetstream::ConsumerTaskConfig {
        compression: false,
        jetstream_hostname: "jetstream2.us-west.bsky.network".into(),
        collections: Vec::new(),
        dids: Vec::new(),
        max_message_size_bytes: None,
        cursor: None,
        require_hello: true,
        zstd_dictionary_location: String::new(),
        user_agent: "nsid-tracker/0.0.1".into(),
    });

    tracing::info!("running jetstream consumer...");
    let cancel_token = CancellationToken::new();
    tokio::spawn({
        let db = db.clone();
        async move {
            consumer
                .register_handler(Arc::new(JetstreamHandler { db }))
                .await
                .unwrap();
            consumer.run_background(cancel_token.clone()).await.unwrap();
        }
    });

    serve(db).await;
}
