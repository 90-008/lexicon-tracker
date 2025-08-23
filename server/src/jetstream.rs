use std::time::Duration;

use anyhow::anyhow;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tokio_websockets::{ClientBuilder, MaybeTlsStream, Message as WsMessage, WebSocketStream};

use crate::error::AppResult;

pub struct JetstreamClient {
    stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    tls_connector: tokio_websockets::Connector,
    urls: Vec<SmolStr>,
}

impl JetstreamClient {
    pub fn new(urls: impl IntoIterator<Item = impl Into<SmolStr>>) -> AppResult<Self> {
        Ok(Self {
            stream: None,
            tls_connector: tokio_websockets::Connector::new()?,
            urls: urls.into_iter().map(Into::into).collect(),
        })
    }

    pub async fn connect(&mut self) -> AppResult<()> {
        for uri in &self.urls {
            let conn_result = ClientBuilder::new()
                .connector(&self.tls_connector)
                .uri(uri)?
                .connect()
                .await;
            match conn_result {
                Ok((stream, _)) => {
                    self.stream = Some(stream);
                    tracing::info!("connected to jetstream {}", uri);
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("failed to connect to jetstream {uri}: {err}");
                }
            };
        }
        Err(anyhow!("failed to connect to any jetstream server").into())
    }

    // automatically retries connection, only returning error if it fails many times
    pub async fn read(&mut self, cancel_token: CancellationToken) -> AppResult<JetstreamEvent> {
        let mut retry = false;
        loop {
            {
                let Some(stream) = self.stream.as_mut() else {
                    return Err(anyhow!("not connected, call .connect() first").into());
                };
                tokio::select! {
                    res = stream.next() => match res {
                        Some(Ok(msg)) => {
                            if let Some(event) = msg
                                .as_text()
                                .and_then(|v| serde_json::from_str::<JetstreamEvent>(v).ok())
                            {
                                return Ok(event);
                            } else if msg.is_ping() {
                                let _ = stream.send(WsMessage::pong(msg.into_payload())).await;
                            } else {
                                return Err(anyhow!("unsupported message type").into());
                            }
                        }
                        Some(Err(err)) => {
                            tracing::error!("jetstream connection errored: {err}");
                            retry = true;
                        }
                        None => retry = true,
                    },
                    _ = cancel_token.cancelled() => {
                        return Err(anyhow!("cancelled").into());
                    }
                }
            }
            // retry until connected
            let mut backoff = Duration::from_secs(1);
            while retry {
                if backoff.as_secs() > 64 {
                    return Err(anyhow!("jetstream connection timed out").into());
                }
                tokio::select! {
                    res = self.connect() => if let Err(err) = res {
                        tracing::error!(
                            { retry_in = %backoff.as_secs() },
                            "couldn't retry jetstream connection: {err}",
                        );
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    },
                    _ = cancel_token.cancelled() => {
                        return Err(anyhow!("cancelled").into());
                    }
                }

                retry = false;
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JetstreamEvent {
    /// Repository commit event (create/update operations)
    Commit {
        /// DID of the repository that was updated
        did: String,
        /// Event timestamp in microseconds since Unix epoch
        time_us: u64,
        /// Event type identifier
        kind: String,

        #[serde(rename = "commit")]
        /// Commit operation details
        commit: JetstreamEventCommit,
    },

    /// Repository delete event
    Delete {
        /// DID of the repository that was updated
        did: String,
        /// Event timestamp in microseconds since Unix epoch
        time_us: u64,
        /// Event type identifier
        kind: String,

        #[serde(rename = "commit")]
        /// Delete operation details
        commit: JetstreamEventDelete,
    },

    /// Identity document update event
    Identity {
        /// DID whose identity was updated
        did: String,
        /// Event timestamp in microseconds since Unix epoch
        time_us: u64,
        /// Event type identifier
        kind: String,

        #[serde(rename = "identity")]
        /// Identity document data
        identity: serde_json::Value,
    },

    /// Account-related event
    Account {
        /// DID of the account
        did: String,
        /// Event timestamp in microseconds since Unix epoch
        time_us: u64,
        /// Event type identifier
        kind: String,

        #[serde(rename = "account")]
        /// Account data
        identity: serde_json::Value,
    },
}

/// Repository commit operation details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JetstreamEventCommit {
    /// Repository revision identifier
    pub rev: String,
    /// Operation type (create, update)
    pub operation: String,
    /// AT Protocol collection name
    pub collection: String,
    /// Record key within the collection
    pub rkey: String,
    /// Content identifier (CID) of the record
    pub cid: String,
    /// Record data as JSON
    pub record: serde_json::Value,
}

/// Repository delete operation details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JetstreamEventDelete {
    /// Repository revision identifier
    pub rev: String,
    /// Operation type (delete)
    pub operation: String,
    /// AT Protocol collection name
    pub collection: String,
    /// Record key that was deleted
    pub rkey: String,
}
