use std::env;
use std::sync::Arc;
use warp::{Filter, Rejection, Reply};
use serde::{Deserialize, Serialize};
use log::{debug, info};
use crate::core::node::Node;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct InsertResponse {
    success: bool,
    message: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct InsertRequest {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct GetResponse {
    success: bool,
    message: String,
    value: String,
}

async fn handle_insert(req: InsertRequest, node: Arc<Node>) -> Result<impl Reply, Rejection> {
    let mut resp: InsertResponse;
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
    let mut resp: GetResponse;
    if let Some(val) = node.get(&key) {
        resp = GetResponse {
            success: true,
            message: "Key found".to_string(),
            value: val.to_string(),
        };
    } else {
        resp = GetResponse {
            success: false,
            message: "Key not found".to_string(),
            value: "".to_string(),
        };
    }
    Ok(warp::reply::json(&resp))
}

fn json_body() -> impl Filter<Extract=(InsertRequest, ), Error=Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

#[tokio::main]
pub async fn main() {
    let args: Vec<String> = env::args().collect();
    let port: u16 = match args.windows(2).find(|w| w[0] == "--p") {
        Some(window) => window[1].parse().unwrap_or(3030),
        None => 3030,
    };

    let node = Arc::new(Node::new());
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

    // Combine routes and start the server
    let routes = route_insert
        .or(route_get);

    info!("Starting server at :{port}");
    warp::serve(routes).run(([127, 0, 0, 1], port)).await;
}