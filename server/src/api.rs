use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use smol_str::SmolStr;

use crate::{db::Db, error::AppResult};

pub async fn serve(db: Arc<Db>) {
    let app = Router::new().route("/events", get(events)).with_state(db);

    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::info!("starting serve on {addr}");
    axum::serve(listener, app).await.unwrap();
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
