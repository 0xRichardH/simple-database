mod app_error;
mod app_server;
mod app_state;
mod handlers;
mod router;
mod scheduler;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use app_server::AppServerBuilder;
use scheduler::Scheduler;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing_subscriber();

    // To run database compaction in the background
    let scheduler = Scheduler::new("./db", 50 * 1024 * 1024);
    tokio::spawn(async move { scheduler.perform().await });

    // Start the Database API server
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let app = router::create().await.context("create router")?;
    let app_server = AppServerBuilder::new(app).with_socket_address(addr).build();

    app_server.start().await.context("start api server")?;
    Ok(())
}

fn init_tracing_subscriber() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "db_server=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}
