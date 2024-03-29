use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use warp::{Filter, Rejection, Reply};
use crate::core::allocator::{Allocator, RegistrationInteractions, StorageInteractions};
use dns_lookup::lookup_addr;
use reqwest::Client;
use crate::clients::node_client::NodeClient;


#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct AllocatorInsertRequest {
    key: String,
    value: String,
}

impl AllocatorInsertRequest {
    pub fn new(k: String, v: String) -> Self {
        Self {
            key: k,
            value: v,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct AllocatorGetResponse {
    success: bool,
    message: String,
    value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct RegisterNodeRequest {
    id: String,
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct GenericResponse {
    success: bool,
    message: String,
}

impl GenericResponse {
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn new(success: bool, message: String) -> Self {
        Self { success, message }
    }

    pub fn init() -> Self {
        Self {
            success: false,
            message: "init".to_string(),
        }
    }
}

impl AllocatorGetResponse {
    pub fn is_success(&self) -> bool { self.success }
    pub fn get_value(&self) -> String { self.value.clone() }
    pub fn get_msg(&self) -> String { self.message.clone() }
}

fn json_body() -> impl Filter<Extract=(AllocatorInsertRequest, ), Error=Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn json_body_register() -> impl Filter<Extract=(RegisterNodeRequest, ), Error=Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

async fn handle_get(key: String, allocator: Arc<Allocator>) -> Result<impl Reply, Rejection> {
    let resp: AllocatorGetResponse;

    match allocator.get(&key).await {
        Ok(val) => {
            resp = AllocatorGetResponse {
                success: true,
                message: "Key found".to_string(),
                value: val,
            };
        }
        Err(_) => {
            resp = AllocatorGetResponse {
                success: false,
                message: "Key not found".to_string(),
                value: "".to_string(),
            };
        }
    }
    Ok(warp::reply::json(&resp))
}


async fn handle_register(req: RegisterNodeRequest, allocator: Arc<Allocator>, remote_addr: Option<SocketAddr>, client: Arc<Client>) -> Result<impl Reply, Rejection> {
    let mut resp = GenericResponse::init();
    let host: String;
    match remote_addr {
        Some(address) => {
            let ip = address.ip();
            match lookup_addr(&ip) {
                Ok(hostname) => {
                    host = hostname
                }
                Err(_) => {
                    resp.success = false;
                    resp.message = "No remote address found".to_string();
                    return Ok(warp::reply::json(&resp));
                }
            }
            let port = address.port();
            let combined_address = format!("http://{host}:{port}");
            match allocator.register_node(NodeClient::new(client, combined_address, req.id)).await {
                Ok(_) => {
                    resp.success = true;
                    resp.message = "Node registration succeeded".to_string();
                }
                Err(_) => {
                    resp.success = false;
                    resp.message = "Node registration failed".to_string();
                }
            }
        }
        None => {
            resp.success = false;
            resp.message = "No remote address found".to_string();
        }
    }
    Ok(warp::reply::json(&resp))
}

async fn handle_insert(req: AllocatorInsertRequest, allocator: Arc<Allocator>) -> Result<impl Reply, Rejection> {
    let resp: GenericResponse;
    let key = req.clone().key;
    let val = req.clone().value;
    match allocator.store(&key, &val).await {
        Ok(stored) => {
            if stored {
                resp = GenericResponse {
                    success: true,
                    message: "Key inserted successfully".to_string(),
                };
                debug!("request succeeded -> {:?} : {:?}", req, resp);
            } else {
                resp = GenericResponse {
                    success: false,
                    message: "Key already exists".to_string(),
                };
                debug!("request failed -> {:?} : {:?}", req, resp);
            }
        }
        Err(e) => {
            resp = GenericResponse {
                success: false,
                message: "Request failed unexpectedly".to_string(),
            };
            debug!("request failed unexpected -> {:?}", e.to_string());
        }
    }
    Ok(warp::reply::json(&resp))
}

#[tokio::main]
pub async fn main() {
    let args: Vec<String> = env::args().collect();
    let port: u16 = match args.windows(2).find(|w| w[0] == "--p") {
        Some(window) => window[1].parse().unwrap_or(3030),
        None => 3030,
    };
    let slots: u32 = match args.windows(2).find(|w| w[0] == "--s") {
        Some(window) => window[1].parse().unwrap_or(20),
        None => 20,
    };

    let allocator = Arc::new(Allocator::new(slots));
    let client = Arc::new(Client::new());
    let allocator_filter = warp::any().map(move || Arc::clone(&allocator));
    let client_filter = warp::any().map(move || Arc::clone(&client));

    // POST /insert
    let route_insert = warp::path("insert")
        .and(warp::path::end())
        .and(json_body())
        .and(allocator_filter.clone())
        .and_then(handle_insert);

    let route_get = warp::path!("get" / String)
        .and(warp::path::end())
        .and(allocator_filter.clone())
        .and_then(handle_get);


    let route_register = warp::path("register")
        .and(warp::path::end())
        .and(json_body_register())
        .and(allocator_filter.clone())
        .and(warp::addr::remote())
        .and(client_filter.clone())
        .and_then(handle_register);

    // Combine routes and start the server
    let routes = route_insert
        .or(route_get)
        .or(route_register);

    info!("Starting server at :{port}");
    warp::serve(routes).run(([127, 0, 0, 1], port)).await;
}