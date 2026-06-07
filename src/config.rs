use std::sync::RwLock;
use dotenv::dotenv;

pub struct Config {
    pub port: u32,
    pub backend_addrs: Vec<String>,
    pub health_check_time: u32
}

impl Config {
    pub fn load_from_env() -> Self {
        let default = Self::default();

        dotenv().ok().expect("Unable to init dotenv");

        let port = match std::env::var("PORT") {
            Ok(port) => port.parse::<u32>().unwrap_or(default.port),
            Err(_) => default.port,
        };
        println!("Starting on port: {port}");

        let all_backends: Vec<String> = std::env::var("BACKENDS")
            .expect("Unable to read BACKENDS from env")
            .split(",")
            .map(|x| String::from(x))
            .collect();
        let backends = RwLock::new(all_backends.clone());
        println!("Backends: {:?}", backends);

        let health_check_time = match std::env::var("HEALTH_CHECK_TIME") {
            Ok(t) => t.parse::<u32>().unwrap_or(default.health_check_time),
            Err(_) => default.health_check_time,
        };
        Config {
            port,
            backend_addrs: all_backends,
            health_check_time
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: 8080,
            backend_addrs: vec![],
            health_check_time: 5
        }
    }
}
