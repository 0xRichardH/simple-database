use axum::{
    http::{StatusCode, Uri},
    Json,
};
use serde::Serialize;

#[derive(Serialize, Default)]
pub struct ErrorResponse {
    error: String,
    message: Option<String>,
}

pub async fn not_found_handler(uri: Uri) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: String::from("not_found"),
            message: Some(format!("Requested path `{}` not found.", uri.path())),
        }),
    )
}
