use anyhow::Result;
use axum::{extract::MatchedPath, http::Request, routing::get, Router};
use tower_http::trace::TraceLayer;

use crate::handlers::prelude::*;

pub fn create() -> Result<Router> {
    let router =
        Router::new()
            .merge(api_router())
            .layer(
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

fn api_router() -> Router {
    Router::new()
        .route("/api/get/:key", get(get_handler))
        .fallback(not_found_handler)
}
