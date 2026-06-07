use dotenv::dotenv;
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    dotenv().ok().expect("Unable to init dotenv");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    println!("Starting on port: {port}");

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("Unable to start TcpListener");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(mut client_stream: tokio::net::TcpStream) {
    let backend_addr = "127.0.0.1:4000";

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
