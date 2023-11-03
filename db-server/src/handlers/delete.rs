use axum::{
    extract::{Path, State},
    Json,
};

use crate::{app_error::AppError, app_state::AppState};

pub async fn delete_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<usize>, AppError> {
    let db = state.db.clone();
    let result = db.lock().await.delete(key.as_bytes()).await?;
    Ok(Json(result))
}
