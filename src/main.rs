use std::{env};
use dotenv::dotenv;

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

async fn handle_connection(mut stream: tokio::net::TcpStream) {
    // Split the bidirectional stream into a read half and a write half
    let (mut reader, mut writer) = stream.split();

    // Automatically copy everything from the reader back into the writer
    match tokio::io::copy(&mut reader, &mut writer).await {
        Ok(bytes_copied) => {
            println!("Connection closed gracefully. Echoed {bytes_copied} bytes.");
        }
        Err(e) => {
            eprintln!("Connection ended with an error: {e}");
        }
    }
}