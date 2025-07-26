use std::{collections::HashMap, net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use anyhow::anyhow;
use axum::{Json, Router, extract::State, http::Request, response::Response, routing::get};
use axum_tws::{Message, WebSocketUpgrade};
use serde::Serialize;
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use tower_http::{
    classify::ServerErrorsFailureClass,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{Instrument, Span, field};

use crate::{
    db::Db,
    error::{AppError, AppResult},
};

pub async fn serve(db: Arc<Db>, cancel_token: CancellationToken) -> AppResult<()> {
    let app = Router::new()
        .route("/events", get(events))
        .route("/stream_events", get(stream_events))
        .route_layer(PropagateRequestIdLayer::x_request_id())
        .route_layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    let span = tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        id = field::Empty,
                        ip = field::Empty,
                    );
                    if let Some(id) = request.headers().get("x-request-id") {
                        span.record("id", String::from_utf8_lossy(id.as_bytes()).deref());
                    }
                    if let Some(real_ip) = request.headers().get("x-real-ip") {
                        span.record("ip", String::from_utf8_lossy(real_ip.as_bytes()).deref());
                    }
                    span
                })
                .on_request(|request: &Request<_>, span: &Span| {
                    let _ = span.enter();
                    tracing::info!("processing")
                })
                .on_response(|response: &Response<_>, latency: Duration, span: &Span| {
                    let _ = span.enter();
                    tracing::info!({code = %response.status().as_u16(), latency = %latency.as_millis()}, "processed")
                })
                .on_eos(())
                .on_failure(|error: ServerErrorsFailureClass, _: Duration, span: &Span| {
                    let _ = span.enter();
                    if matches!(error, ServerErrorsFailureClass::StatusCode(status_code) if status_code.is_server_error()) || matches!(error, ServerErrorsFailureClass::Error(_)) {
                        tracing::error!("server error: {}", error.to_string().to_lowercase());
                    };
                }),
        )
        .route_layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
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
    let span = tracing::info_span!(parent: Span::current(), "ws");
    ws.on_upgrade(move |mut socket| {
        (async move {
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
        .instrument(span)
    })
}
