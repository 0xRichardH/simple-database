mod app_server;
mod handlers;
mod router;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use app_server::AppServerBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let app = router::create().context("create router")?;
    let app_server = AppServerBuilder::new(app).with_socket_address(addr).build();

    app_server.start().await.context("start api server")?;
    Ok(())
}
