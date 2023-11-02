use anyhow::Result;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::app_state::AppState;

pub async fn get_handler(State(state): State<AppState>, Path(key): Path<String>) -> Result<()> {
    // FIXME: Database is not thread safe
    // let db = state.db.clone().lock()?;
    Ok(())
}
