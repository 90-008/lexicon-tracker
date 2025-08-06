use std::{
    collections::HashMap,
    fmt::Display,
    net::SocketAddr,
    ops::{Bound, Deref, RangeBounds},
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::Request,
    response::Response,
    routing::get,
};
use axum_tws::{Message, WebSocketUpgrade};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use tower_http::{
    classify::ServerErrorsFailureClass,
    compression::CompressionLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{Instrument, Span, field};

use crate::{
    db::Db,
    error::{AppError, AppResult},
};

struct LatencyMillis(u128);

impl From<Duration> for LatencyMillis {
    fn from(duration: Duration) -> Self {
        LatencyMillis(duration.as_millis())
    }
}

impl Display for LatencyMillis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

pub async fn serve(db: Arc<Db>, cancel_token: CancellationToken) -> AppResult<()> {
    let app = Router::new()
        .route("/events", get(events))
        .route("/stream_events", get(stream_events))
        .route("/hits", get(hits))
        .route("/since", get(since))
        .route_layer(CompressionLayer::new().br(true).deflate(true).gzip(true).zstd(true))
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
                .on_request(|_request: &Request<_>, span: &Span| {
                    let _ = span.enter();
                    tracing::info!("processing")
                })
                .on_response(|response: &Response<_>, latency: Duration, span: &Span| {
                    let _ = span.enter();
                    tracing::info!({code = %response.status().as_u16(), latency = %LatencyMillis::from(latency)}, "processed")
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

#[derive(Debug, Deserialize)]
struct HitsQuery {
    nsid: SmolStr,
    from: Option<u64>,
    to: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Hit {
    timestamp: u64,
    deleted: bool,
}

const MAX_HITS: usize = 100_000;

#[derive(Debug)]
struct HitsRange {
    from: Bound<u64>,
    to: Bound<u64>,
}

impl RangeBounds<u64> for HitsRange {
    fn start_bound(&self) -> Bound<&u64> {
        self.from.as_ref()
    }

    fn end_bound(&self) -> Bound<&u64> {
        self.to.as_ref()
    }
}

async fn hits(
    State(db): State<Arc<Db>>,
    Query(params): Query<HitsQuery>,
) -> AppResult<Json<Vec<Hit>>> {
    let from = params.to.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let to = params.from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let maybe_hits = db
        .get_hits(&params.nsid, HitsRange { from, to })
        .take(MAX_HITS);
    let mut hits = Vec::with_capacity(maybe_hits.size_hint().0);

    for maybe_hit in maybe_hits {
        let hit = maybe_hit?;
        let hit_data = hit.access();

        hits.push(Hit {
            timestamp: hit.timestamp,
            deleted: hit_data.deleted,
        });
    }

    Ok(Json(hits))
}

async fn stream_events(db: State<Arc<Db>>, ws: WebSocketUpgrade) -> Response {
    let span = tracing::info_span!(parent: Span::current(), "ws");
    ws.on_upgrade(move |mut socket| {
        (async move {
            let mut listener = db.new_listener();
            let mut data = Events {
                events: HashMap::<SmolStr, NsidCount>::with_capacity(10),
                per_second: 0,
            };
            let mut updates = 0;
            while let Ok((nsid, counts)) = listener.recv().await {
                data.events.insert(
                    nsid,
                    NsidCount {
                        count: counts.count,
                        deleted_count: counts.deleted_count,
                        last_seen: counts.last_seen,
                    },
                );
                updates += 1;
                // send 20 times every second max
                data.per_second = db.eps();
                if updates >= data.per_second / 16 {
                    let msg = serde_json::to_string(&data).unwrap();
                    let res = socket.send(Message::text(msg)).await;
                    data.events.clear();
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

#[derive(Debug, Serialize)]
struct Since {
    since: u64,
}

async fn since(db: State<Arc<Db>>) -> AppResult<Json<Since>> {
    Ok(Json(Since {
        since: db.tracking_since()?,
    }))
}
