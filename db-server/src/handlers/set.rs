use axum::{
    extract::{Path, State},
    Json,
};
use db_engine::DbEntry;

use crate::{app_error::AppError, app_state::AppState};

pub async fn set_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    value: String, // get the value from request body
) -> Result<Json<usize>, AppError> {
    let db = state.db.clone();
    // FIXME
    // let result = db.set(key.as_bytes(), value.as_bytes()).await?;
    let result = 1;
    Ok(Json(result))
}
