use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use serde::Serialize;

use crate::app_state::AppState;

#[derive(Serialize)]
pub struct Entry {
    key: String,
    value: String,
    timestamp: u128,
}

pub async fn get_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Json<Option<Entry>> {
    let db = Arc::clone(&state.db);
    let db_entry = db.lock().await.get(key.as_bytes()).await;

    let mut entry = None;
    if let Some(data) = db_entry {
        entry = Some(Entry {
            key: String::from_utf8_lossy(&data.key).into_owned(),
            value: String::from_utf8_lossy(&data.value).into_owned(),
            timestamp: data.timestamp,
        })
    }

    Json(entry)
}
