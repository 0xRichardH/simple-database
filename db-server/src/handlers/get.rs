use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
};

pub async fn get_handler(Path(key): Path<String>) -> Response {
    (StatusCode::OK, "Ok").into_response()
}
