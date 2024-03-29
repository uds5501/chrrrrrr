use std::env;
use std::string::ToString;
use log::debug;

mod server;
mod core;
mod clients;
mod errors;

const DEFAULT_MODE: &str = "NODE";

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let mode: String = match args.windows(2).find(|w| w[0] == "--mode") {
        Some(window) => window[1].parse().unwrap_or(DEFAULT_MODE.to_string()),
        None => DEFAULT_MODE.to_string(),
    };
    debug!("mode found = {mode}");
    if mode == DEFAULT_MODE || mode.to_ascii_lowercase() == "n" {
        server::node_server::main()
    } else if mode == "ALLOCATOR" || mode.to_ascii_lowercase() == "a" {
        server::allocater_server::main()
    }
}
