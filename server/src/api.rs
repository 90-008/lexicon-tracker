use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::anyhow;
use axum::{Json, Router, extract::State, response::Response, routing::get};
use axum_tws::{Message, WebSocketUpgrade};
use serde::Serialize;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    db::Db,
    error::{AppError, AppResult},
};

pub async fn serve(db: Arc<Db>, cancel_token: CancellationToken) -> AppResult<()> {
    let app = Router::new()
        .route("/events", get(events))
        .route("/stream_events", get(stream_events))
        .route_layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .route_layer(TraceLayer::new_for_http())
        .route_layer(PropagateRequestIdLayer::x_request_id())
        .with_state(db);

    let addr = SocketAddr::from((
        [0, 0, 0, 0],
        std::env::var("PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(3713),
    ));
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!("starting serve on {addr}");
    tokio::select! {
        res = axum::serve(listener, app) => res.map_err(AppError::from),
        _ = cancel_token.cancelled() => Err(anyhow!("cancelled").into()),
    }
}

#[derive(Serialize)]
struct NsidCount {
    count: u128,
    deleted_count: u128,
    last_seen: u64,
}
#[derive(Serialize)]
struct Events {
    per_second: usize,
    events: HashMap<SmolStr, NsidCount>,
}
#[derive(Serialize)]
struct EventsRef<'a> {
    per_second: usize,
    events: &'a HashMap<SmolStr, NsidCount>,
}
async fn events(db: State<Arc<Db>>) -> AppResult<Json<Events>> {
    let mut events = HashMap::new();
    for result in db.get_counts() {
        let (nsid, counts) = result?;
        events.insert(
            nsid,
            NsidCount {
                count: counts.count,
                deleted_count: counts.deleted_count,
                last_seen: counts.last_seen,
            },
        );
    }
    Ok(Json(Events {
        events,
        per_second: db.eps(),
    }))
}

async fn stream_events(db: State<Arc<Db>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(async move |mut socket| {
        let mut listener = db.new_listener();
        let mut buffer = HashMap::<SmolStr, NsidCount>::with_capacity(10);
        let mut updates = 0;
        while let Ok((nsid, counts)) = listener.recv().await {
            buffer.insert(
                nsid,
                NsidCount {
                    count: counts.count,
                    deleted_count: counts.deleted_count,
                    last_seen: counts.last_seen,
                },
            );
            updates += 1;
            // send 10 times every second max
            let per_second = db.eps();
            if updates >= per_second / 10 {
                let msg = serde_json::to_string(&EventsRef {
                    events: &buffer,
                    per_second,
                })
                .unwrap();
                let res = socket.send(Message::text(msg)).await;
                buffer.clear();
                updates = 0;
                if let Err(err) = res {
                    tracing::error!("error sending event: {err}");
                    break;
                }
            }
        }
    })
}
