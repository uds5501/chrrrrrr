use std::collections::HashMap;
use std::env;

use std::string::ToString;
use std::sync::Arc;
use warp::{Filter, Rejection, Reply};
use serde::{Deserialize, Serialize};
use log::{debug, info};
use reqwest::Client;
use crate::clients::allocator_client::{AllocatorClient, AllocatorInteractions};
use crate::core::node::Node;


const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:3030";
const DEFAULT_PORT: u16 = 3031;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct InsertResponse {
    success: bool,
    message: String,
}

impl InsertResponse {
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn new(success: bool, message: String) -> Self {
        Self { success, message }
    }
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct MigrateResponse {
    success: bool,
    message: String,
    data: Option<HashMap<String, String>>,
}

impl MigrateResponse {
    pub fn is_success(&self) -> bool {
        self.success
    }
    pub fn get_data(&self) -> Option<HashMap<String, String>> {
        self.data.clone()
    }
    pub fn get_msg(&self) -> String { self.message.clone() }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct InsertRequest {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct MigrateRequest {
    start: u32,
    end: u32,
    n: u32,
}

impl InsertRequest {
    pub fn new(k: String, v: String) -> Self {
        Self {
            key: k,
            value: v,
        }
    }
}

impl MigrateRequest {
    pub fn new(start: u32, end: u32, n: u32) -> Self {
        Self {
            start,
            end,
            n,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct GetResponse {
    success: bool,
    message: String,
    value: Option<String>,
}

impl GetResponse {
    pub fn is_success(&self) -> bool { self.success }
    pub fn get_value(&self) -> String { self.value.as_ref().unwrap().clone() }
    pub fn get_msg(&self) -> String { self.message.clone() }
}

async fn handle_insert(req: InsertRequest, node: Arc<Node>) -> Result<impl Reply, Rejection> {
    let resp: InsertResponse;
    let key = req.clone().key;
    let val = req.clone().value;
    if node.insert_key(key, val) {
        resp = InsertResponse {
            success: true,
            message: "Key inserted successfully".to_string(),
        };
        debug!("request succeeded -> {:?} : {:?}", req, resp);
    } else {
        resp = InsertResponse {
            success: false,
            message: "Key already exists".to_string(),
        };
        debug!("request failed -> {:?} : {:?}", req, resp);
    }
    Ok(warp::reply::json(&resp))
}

// Handler for the get endpoint
async fn handle_get(key: String, node: Arc<Node>) -> Result<impl Reply, Rejection> {
    let resp: GetResponse;
    if let Some(val) = node.get(&key) {
        resp = GetResponse {
            success: true,
            message: "Key found".to_string(),
            value: Some(val.to_string()),
        };
    } else {
        resp = GetResponse {
            success: false,
            message: "Key not found".to_string(),
            value: None,
        };
    }
    Ok(warp::reply::json(&resp))
}

async fn handle_migrate(req: MigrateRequest, node: Arc<Node>) -> Result<impl Reply, Rejection> {
    let data = node.migrate(req.start, req.end, req.n);

    let response = MigrateResponse {
        success: true,
        message: "Migration successful".to_string(),
        data: Some(data),
    };

    Ok(warp::reply::json(&response))
}

fn json_body() -> impl Filter<Extract=(InsertRequest, ), Error=Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn migrate_body() -> impl Filter<Extract=(MigrateRequest, ), Error=Rejection> + Clone {
    warp::body::json()
}

#[tokio::main]
pub async fn main() {
    let args: Vec<String> = env::args().collect();
    let port: u16 = match args.windows(2).find(|w| w[0] == "--p") {
        Some(window) => window[1].parse().unwrap_or(DEFAULT_PORT),
        None => DEFAULT_PORT,
    };
    let server_url: String = match args.windows(2).find(|w| w[0] == "--s") {
        Some(window) => window[1].parse().unwrap_or(DEFAULT_SERVER_URL.to_string()),
        None => DEFAULT_SERVER_URL.to_string(),
    };
    let alloc_client = Arc::new(Client::new());
    let allocator_client = AllocatorClient::new(alloc_client, server_url.clone());

    let node = Arc::new(Node::new());
    let node_for_registration = Arc::clone(&node);
    let node_filter = warp::any().map(move || Arc::clone(&node));

    // POST /insert
    let route_insert = warp::path("insert")
        .and(warp::path::end())
        .and(json_body())
        .and(node_filter.clone())
        .and_then(handle_insert);

    let route_get = warp::path!("get" / String)
        .and(warp::path::end())
        .and(node_filter.clone())
        .and_then(handle_get);

    let route_migrate = warp::path("migrate")
        .and(warp::path::end())
        .and(migrate_body())
        .and(node_filter.clone())
        .and_then(handle_migrate);

    // Combine routes and start the server
    let routes = route_insert
        .or(route_get)
        .or(route_migrate);

    info!("Starting server at :{:?}", &port);
    info!("Registering to allocator ({:?})", &server_url);
    match allocator_client.register(node_for_registration.get_id()).await {
        Ok(registered) => {
            info!("Registration status - {registered}");
        }
        Err(e) => {
            debug!("Some unexpected error occurred - {:?}", e);
        }
    }
    warp::serve(routes).run(([127, 0, 0, 1], port)).await;
}