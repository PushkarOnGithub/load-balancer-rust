use dotenv::dotenv;
use std::{
    env,
    ops::Add,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct RouterState {
    backends: Vec<String>,
    current_index: Mutex<usize>,
}

#[tokio::main]
async fn main() {
    dotenv().ok().expect("Unable to init dotenv");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    println!("Starting on port: {port}");

    let backends: Vec<String> = env::var("BACKENDS")
        .expect("Unable to read BACKENDS from env")
        .split(",")
        .map(|x| String::from(x))
        .collect();

    println!("Backends: {:?}", backends);

    let router = Arc::new(RouterState {
        backends,
        current_index: Mutex::new(0),
    });

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("Unable to start TcpListener");

    loop {
        let router_clone = Arc::clone(&router);

        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { handle_connection(stream, router_clone).await });
    }
}

async fn handle_connection(mut client_stream: tokio::net::TcpStream, router: Arc<RouterState>) {
    let backend_addr = {
        let mut index = router
            .current_index
            .try_lock()
            .expect("Unable to lock mutex");
        let backend = router.backends.get(*index).unwrap().clone();
        *index = index.add(1) % router.backends.len();
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
