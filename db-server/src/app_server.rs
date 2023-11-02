use std::net::SocketAddr;

use anyhow::Result;
use axum::Router;
use tokio::signal;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct AppServer {
    router: Router,
    socket_address: SocketAddr,
}

impl AppServer {
    pub async fn start(self) -> Result<()> {
        init_tracing_subscriber();

        tracing::info!("Listening on {}", self.socket_address);

        axum::Server::bind(&self.socket_address)
            .serve(
                self.router
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        Ok(())
    }
}

pub struct AppServerBuilder(AppServer);

impl AppServerBuilder {
    pub fn new(router: Router) -> Self {
        let default_socket_address = SocketAddr::from(([0, 0, 0, 0], 8080));
        let app_server = AppServer {
            router,
            socket_address: default_socket_address,
        };
        Self(app_server)
    }

    pub fn with_socket_address(mut self, socket_address: SocketAddr) -> Self {
        self.0.socket_address = socket_address;
        self
    }

    pub fn build(self) -> AppServer {
        self.0
    }
}

fn init_tracing_subscriber() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
        },
        _ = terminate => {
        },
    }

    tracing::info!("signal received, starting graceful shutdown");
}
