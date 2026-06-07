use crate::types::RouterState;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// handles a tcp connection
pub async fn handle_connection(mut client_stream: tokio::net::TcpStream, router: Arc<RouterState>) {
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
