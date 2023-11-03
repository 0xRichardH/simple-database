use anyhow::{Context, Result};
use axum::{
    extract::MatchedPath,
    http::Request,
    routing::{delete, get, post},
    Router,
};
use tower_http::trace::TraceLayer;

use crate::{app_state::AppState, handlers::prelude::*};

pub async fn create() -> Result<Router> {
    let api_state = AppState::new().await.context("create API AppState")?;

    let router = Router::new().merge(api_router(api_state)).layer(
        TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
            // Log the matched route's path (with placeholders not filled in).
            // Use request.uri() or OriginalUri if you want the real path.
            let matched_path = request
                .extensions()
                .get::<MatchedPath>()
                .map(MatchedPath::as_str);

            tracing::info_span!(
                "http_request",
                method = ?request.method(),
                matched_path,
                some_other_field = tracing::field::Empty,
            )
        }),
    );

    Ok(router)
}

fn api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/entry/:key", get(get_handler))
        .route("/api/entry/:key", post(set_handler))
        .route("/api/entry/:key", delete(delete_handler))
        .with_state(state)
        .fallback(not_found_handler)
}
