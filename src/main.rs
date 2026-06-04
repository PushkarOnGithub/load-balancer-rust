use std::env;
use dotenv::dotenv;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
    let buf_reader = BufReader::new(&mut stream);
    let mut lines = buf_reader.lines();

    let mut http_request = Vec::new();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() {
            break
        };

        http_request.push(line);
    }

    let status_code = "HTTP/1.1 200 OK";

    let body = tokio::fs::read_to_string("src/hello.html").await.unwrap();
    let length = body.len();

    let response = format!("{status_code}\r\nContent-Length: {length}\r\n\r\n{body}");
    stream.write_all(response.as_bytes()).await.unwrap();

    println!("Request: {http_request:#?}");
}
