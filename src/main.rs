pub mod handler;
pub mod health_check;
pub mod types;
pub mod config;

use std::sync::{Arc, Mutex, RwLock};

use crate::health_check::healthcheck;
use crate::types::RouterState;
use crate::{handler::handle_connection, config::Config};

#[tokio::main]
async fn main() {
    let config = Config::load_from_env();
    let router = Arc::new(RouterState {
        backends: RwLock::new(config.backend_addrs.clone()),
        current_index: Mutex::new(Some(0)),
    });

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", config.port))
        .await
        .expect("Unable to start TcpListener");



    // Spawn ONE health check task before accepting connections
    let router_clone = Arc::clone(&router);
    tokio::spawn(async move { healthcheck(config.backend_addrs, router_clone, config.health_check_time).await });

    loop {
        let router_clone = Arc::clone(&router);

        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { handle_connection(stream, router_clone).await });
    }
}
