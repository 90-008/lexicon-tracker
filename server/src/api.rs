use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Request, State, WebSocketUpgrade, ws::Message},
    middleware::Next,
    response::Response,
    routing::get,
};
use serde::Serialize;
use smol_str::SmolStr;

use crate::{db::Db, error::AppResult};

pub async fn serve(db: Arc<Db>) {
    let app = Router::new()
        .route("/events", get(events))
        .route("/stream_events", get(stream_events))
        .route_layer(axum::middleware::from_fn(log))
        .with_state(db);

    let addr = SocketAddr::from((
        [0, 0, 0, 0],
        std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(3713),
    ));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("starting serve on {addr}");
    axum::serve(listener, app).await.unwrap();
}

async fn log(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let resp = next.run(req).await;
    if resp.status().is_server_error() {
        tracing::error!("{method} {uri} ({})", resp.status());
    } else {
        tracing::info!("{method} {uri} ({})", resp.status());
    }
    resp
}

#[derive(Serialize)]
struct NsidCount {
    nsid: SmolStr,
    count: u128,
    deleted_count: u128,
    last_seen: u64,
}
#[derive(Serialize)]
struct Events {
    events: Vec<NsidCount>,
}
async fn events(db: State<Arc<Db>>) -> AppResult<Json<Events>> {
    let mut events = Vec::new();
    for result in db.get_counts() {
        let (nsid, counts) = result?;
        events.push(NsidCount {
            nsid,
            count: counts.count,
            deleted_count: counts.deleted_count,
            last_seen: counts.last_seen,
        })
    }
    events.sort_unstable_by(|a, b| b.count.cmp(&a.count));
    Ok(Json(Events { events }))
}

async fn stream_events(db: State<Arc<Db>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(async move |mut socket| {
        let mut listener = db.new_listener();
        while let Ok((nsid, counts)) = listener.recv().await {
            let res = socket
                .send(Message::Binary(
                    serde_json::to_vec(&NsidCount {
                        nsid,
                        count: counts.count,
                        deleted_count: counts.deleted_count,
                        last_seen: counts.last_seen,
                    })
                    .unwrap()
                    .into(),
                ))
                .await;
            if let Err(err) = res {
                tracing::error!("error sending event: {err}");
                break;
            }
        }
    })
}
