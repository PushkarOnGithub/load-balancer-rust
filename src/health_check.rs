use crate::types::RouterState;
use std::sync::Arc;

/// Perform health check every `N` seconds on `all_backends` and update state in `router`
pub async fn healthcheck(all_backends: Vec<String>, router: Arc<RouterState>, n: u32) {
    // keep polling using loop
    loop {
        // loop over the available backends and update the list to available ones
        let mut active_backends = vec![];

        for backend_addr in all_backends.clone() {
            if is_healthy(&backend_addr).await {
                active_backends.push(backend_addr);
            }
        }

        {
            let mut idx = router.current_index.lock().expect("Unable to lock Mutex");
            let mut guard = router
                .backends
                .write()
                .expect("Unable to get lock on RwLock");

            if active_backends.len() == 0 {
                *idx = None;
            } else {
                *idx = Some(0);
            }
            println!("Updated backends to: {:?}", active_backends);
            *guard = active_backends;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(n.into())).await;
    }
}

/// returns true if a backend_addr is reachable else false
async fn is_healthy(backend_addr: &str) -> bool {
    tokio::net::TcpStream::connect(backend_addr).await.is_ok()
}
