use std::error::Error;
use std::sync::Arc;
use async_trait::async_trait;
use reqwest::Client;
use crate::server::node_server::{GetResponse, InsertRequest, InsertResponse};
use crate::errors::node_get_error::NodeGetError;

#[async_trait]
pub(crate) trait NodeInteractions {
    async fn insert_key(&self, key: String, val: String) -> Result<bool, Box<dyn Error>>;
    async fn get(&self, key: String) -> Result<String, Box<dyn Error>>;
}

pub(crate) struct NodeMetadata {
    client: Arc<Client>,
    address: String,
    id: u32,
}

impl NodeMetadata {
    pub fn new(client: Arc<Client>, address: String, id: u32) -> Self {
        Self {
            client,
            address,
            id,
        }
    }
}

#[async_trait]
impl NodeInteractions for NodeMetadata {
    async fn insert_key(&self, key: String, val: String) -> Result<bool, Box<dyn Error>> {
        let request_url = format!("{}/insert", self.address);
        println!("{}", request_url);
        let resp = self.client
            .post(request_url.clone())
            .json(&InsertRequest::new(key, val))
            .send().await?;
        let data: InsertResponse = resp.json().await?;
        Ok(data.is_success())
    }

    async fn get(&self, key: String) -> Result<String, Box<dyn Error>> {
        let request_url = format!("{}/get/{}", self.address, key);
        println!("{}", request_url);
        let resp = self.client
            .get(request_url.clone())
            .send().await?;
        let data: GetResponse = resp.json().await?;
        if data.is_success() {
            return Ok(data.get_value());
        }
        Err(Box::new(NodeGetError::new(data.get_msg())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use httpmock::prelude::*;
    use reqwest::Client;
    use serde_json::json;

    use crate::clients::node_client::{NodeInteractions, NodeMetadata};
    use crate::server::node_server::InsertResponse;

    #[tokio::test]
    async fn test_insert_call_success() {
        let server = MockServer::start();

        let insert_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/insert");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Key inserted"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeMetadata {
            client,
            address: server.url(""),
            id: 0,
        };
        assert_eq!(true, node.insert_key("foo".to_string(), "bar".to_string()).await.unwrap());
        insert_mock.assert();
    }

    #[tokio::test]
    async fn test_insert_call_fail() {
        let server = MockServer::start();

        let insert_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/insert");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Key insertion failed"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeMetadata {
            client,
            address: server.url(""),
            id: 0,
        };
        assert_eq!(false, node.insert_key("foo".to_string(), "bar".to_string()).await.unwrap());
        insert_mock.assert();
    }

    #[tokio::test]
    async fn test_get_call_succeeds() {
        let server = MockServer::start();

        let insert_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/get/foo");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Key present", "value": "bar"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeMetadata {
            client,
            address: server.url(""),
            id: 0,
        };
        assert_eq!("bar", node.get("foo".to_string()).await.unwrap());
        insert_mock.assert();
    }

    #[tokio::test]
    async fn test_get_call_fails() {
        let server = MockServer::start();

        let insert_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/get/foo");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Key not present", "value": "bar"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeMetadata {
            client,
            address: server.url(""),
            id: 0,
        };
        assert_eq!("bar", node.get("foo".to_string()).await.unwrap());
        insert_mock.assert();
    }
}
