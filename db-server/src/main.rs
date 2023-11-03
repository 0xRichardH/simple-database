mod app_error;
mod app_server;
mod app_state;
mod handlers;
mod router;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use app_server::AppServerBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let app = router::create().await.context("create router")?;
    let app_server = AppServerBuilder::new(app).with_socket_address(addr).build();

    app_server.start().await.context("start api server")?;
    Ok(())
}
