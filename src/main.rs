use dotenv::dotenv;
use std::{
    env,
    sync::{Arc, Mutex, RwLock},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RouterState {
    backends: Arc<RwLock<Vec<String>>>,
    current_index: Mutex<Option<usize>>,
}

#[tokio::main]
async fn main() {
    dotenv().ok().expect("Unable to init dotenv");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    println!("Starting on port: {port}");

    let all_backends: Vec<String> = env::var("BACKENDS")
        .expect("Unable to read BACKENDS from env")
        .split(",")
        .map(|x| String::from(x))
        .collect();

    let backends = Arc::new(RwLock::new(all_backends.clone()));

    println!("Backends: {:?}", backends);

    let router = Arc::new(RouterState {
        backends,
        current_index: Mutex::new(Some(0)),
    });

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("Unable to start TcpListener");

    // Spawn ONE health check task before accepting connections
    let router_clone = Arc::clone(&router);
    tokio::spawn(async move { healthcheck(all_backends, router_clone).await });

    loop {
        let router_clone = Arc::clone(&router);

        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { handle_connection(stream, router_clone).await });
    }
}

async fn handle_connection(mut client_stream: tokio::net::TcpStream, router: Arc<RouterState>) {
    let backend_addr = {
        let mut index = router.current_index.lock().expect("Unable to lock mutex");

        let backends_guard = router.backends.read().unwrap();

        if backends_guard.is_empty() {
            eprintln!("503 Service Unavailable: No backends online to serve request.");
            return; // This gracefully ends the task and disconnects the client
        }

        let idx = match *index {
            Some(i) => i,
            None => {
                *index = Some(0);
                0
            }
        };

        let backend = backends_guard
            .get(idx)
            .expect("No backend available to serve request")
            .clone();

        *index = Some((idx + 1) % backends_guard.len());
        backend
    };

    println!("Using {} backend", backend_addr);
    let mut backend_stream = match tokio::net::TcpStream::connect(backend_addr).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to backend: {e}");
            return;
        }
    };

    // Split both streams
    let (mut client_read, mut client_write) = client_stream.split();
    let (mut backend_read, mut backend_write) = backend_stream.split();

    let mut client_buf = [0; 8192];
    let mut backend_buf = [0; 8192];

    loop {
        tokio::select! {
            // client sent some data
            result = client_read.read(&mut client_buf) => {
                match result {
                    Ok(0) => {
                        println!("Client disconnected");
                        break; // EOF reached, exit the loop
                    },
                    Ok(n) => {
                        println!("Client sent {} bytes -> echoing", n);
                        if let Err(e) = backend_write.write_all(&client_buf[..n]).await {
                            eprintln!("Failed to write to backend error: {}", e);
                            break;
                        };
                    },
                    Err(e) => {
                        eprintln!("Error while reading client stream error: {}", e);
                        break;
                    }
                }
            }

            // backend sent some data
            result = backend_read.read(&mut backend_buf) => {
                match result {
                    Ok(0) => {
                        println!("Backend disconnected");
                        break; // EOF reached, exit the loop
                    },

                    Ok(n) => {
                        println!("Backend sent {} bytes -> echoing", n);
                        if let Err(e) = client_write.write_all(&backend_buf[..n]).await {
                            eprintln!("Failed to write to backend, error: {}", e);
                            break;
                        };
                    },
                    Err(e) => {
                        eprintln!("Error while reading backend stream error: {}", e);
                        break;
                    }
                }
            }
        }
    }
}

async fn healthcheck(all_backends: Vec<String>, router: Arc<RouterState>) {
    // keep polling using loop
    loop {
        // loop over the available backends and update the list to available ones
        let mut active_backends = vec![];

        for backend_addr in all_backends.clone() {
            if health(&backend_addr).await {
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

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn health(backend_addr: &str) -> bool {
    tokio::net::TcpStream::connect(backend_addr).await.is_ok()
}
