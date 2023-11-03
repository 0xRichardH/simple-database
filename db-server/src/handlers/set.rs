use axum::{
    extract::{Path, State},
    Json,
};

use crate::{app_error::AppError, app_state::AppState};

pub async fn set_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
    value: String, // get the value from request body
) -> Result<Json<usize>, AppError> {
    let db = state.db.clone();
    let result = db
        .lock()
        .await
        .set(key.as_bytes(), value.as_bytes())
        .await?;
    Ok(Json(result))
}
