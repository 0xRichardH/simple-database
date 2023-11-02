use axum::extract::{Path, State};

use crate::{app_error::AppError, app_state::AppState};

pub async fn get_handler(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<(), AppError> {
    // FIXME: Database is not thread safe
    // let db = state.db.clone().lock()?;
    Ok(())
}
