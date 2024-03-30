use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use async_trait::async_trait;
use reqwest::Client;
use crate::errors::migration_error::MigrationError;
use crate::server::node_server::{GetResponse, InsertRequest, InsertResponse, MigrateRequest, MigrateResponse};
use crate::errors::node_get_error::NodeGetError;

#[async_trait]
pub(crate) trait NodeInteractions {
    async fn insert_key(&self, key: String, val: String) -> Result<bool, Box<dyn Error>>;
    async fn get(&self, key: String) -> Result<String, Box<dyn Error>>;
    async fn fetch_migrated(&self, start: u32, end: u32, n: u32) -> Result<Option<Arc<HashMap<String, String>>>, Box<dyn Error + Send + Sync>>;
}

#[derive(Debug)]
pub(crate) struct NodeClient {
    client: Arc<Client>,
    address: String,
    id: String,
}

impl Clone for NodeClient {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            address: self.address.clone(),
            id: self.id.clone(),
        }
    }
}

impl NodeClient {
    pub fn new(client: Arc<Client>, address: String, id: String) -> Self {
        Self {
            client,
            address,
            id,
        }
    }
    pub fn get_id(&self) -> &String {
        return &self.id;
    }
}

#[async_trait]
impl NodeInteractions for NodeClient {
    async fn insert_key(&self, key: String, val: String) -> Result<bool, Box<dyn Error>> {
        let request_url = format!("{}/insert", self.address);
        let resp = self.client
            .post(request_url)
            .json(&InsertRequest::new(key, val))
            .send().await?;
        let data: InsertResponse = resp.json().await?;
        Ok(data.is_success())
    }

    async fn get(&self, key: String) -> Result<String, Box<dyn Error>> {
        let request_url = format!("{}/get/{}", self.address, key);
        let resp = self.client
            .get(request_url)
            .send().await?;
        let data: GetResponse = resp.json().await?;
        if data.is_success() {
            return Ok(data.get_value());
        }
        Err(Box::new(NodeGetError::new(data.get_msg())))
    }

    async fn fetch_migrated(&self, start: u32, end: u32, n: u32) -> Result<Option<Arc<HashMap<String, String>>>, Box<dyn Error + Send + Sync>> {
        let request_url = format!("{}/migrate", self.address);
        let resp = self.client
            .post(request_url.clone())
            .json(&MigrateRequest::new(start, end, n))
            .send().await?;
        let response_json: MigrateResponse = resp.json().await?;
        if response_json.is_success() {
            return match response_json.get_data() {
                Some(hmap) => {
                    Ok(Some(Arc::new(hmap)))
                }
                None => {
                    Ok(None)
                }
            };
        }
        Err(Box::new(MigrationError::new(response_json.get_msg())))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use httpmock::prelude::*;
    use reqwest::Client;
    use serde_json::json;

    use crate::clients::node_client::{NodeInteractions, NodeClient};

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
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
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
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
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
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
        };
        assert_eq!("bar", node.get("foo".to_string()).await.unwrap());
        insert_mock.assert();
    }

    #[tokio::test]
    async fn test_get_call_fails() {
        let server = MockServer::start();

        let get_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/get/foo");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Key not present"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
        };
        let actual = node.get("foo".to_string()).await.unwrap_err();
        assert_eq!("Node fetch failed with error: Key not present".to_string(), actual.to_string());
        get_mock.assert();
    }

    #[tokio::test]
    async fn test_migrate_call_fails() {
        let server = MockServer::start();

        let migrate_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Migration failed"}));
        });

        let client = Arc::new(Client::new());
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
        };
        let actual = node.fetch_migrated(0, 1, 1).await.unwrap_err();
        assert_eq!("Key migration failed with error: Migration failed".to_string(), actual.to_string());
        migrate_mock.assert();
    }

    #[tokio::test]
    async fn test_migrate_call_success() {
        let server = MockServer::start();

        let migrate_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Migration passed",
                    "data": {
                    "k1": "v1",
                    "k2": "v2"
                }}));
        });

        let client = Arc::new(Client::new());
        let node = NodeClient {
            client,
            address: server.url(""),
            id: "0".to_string(),
        };
        let actual = node.fetch_migrated(0, 1, 1).await.unwrap().unwrap();
        let mut expected: HashMap<String, String> = HashMap::new();
        expected.insert("k1".to_string(), "v1".to_string());
        expected.insert("k2".to_string(), "v2".to_string());
        assert_eq!(Arc::new(expected), actual);
        migrate_mock.assert();
    }
}
