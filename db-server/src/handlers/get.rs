use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use db_engine::DbEntry;

use crate::app_state::AppState;

pub async fn get_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Json<Option<DbEntry>> {
    let db = Arc::clone(&state.db);
    let entry = db.get(key.as_bytes()).await;
    Json(entry)
}
