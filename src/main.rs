use std::{
    env, fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
};

use dotenv::dotenv;

fn main() {
    dotenv().ok().expect("Unable to init dotenv");

    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    println!("Starting on port: {port}");

    let listener =
        TcpListener::bind(format!("127.0.0.1:{port}")).expect("Unable to initialise TcpListener");

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        println!("Connection Established");

        handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&stream);

    let http_request: Vec<_> = buf_reader
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();

    let status_code = "HTTP/1.1 200 OK";

    let body = fs::read_to_string("src/hello.html").unwrap();
    let length = body.len();

    let response = format!("{status_code}\r\nContent-Length{length}\r\n\r\n{body}");
    stream.write_all(response.as_bytes()).unwrap();

    println!("Request: {http_request:#?}");
}
