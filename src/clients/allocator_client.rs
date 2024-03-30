use std::error::Error;
use std::sync::Arc;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[async_trait]
pub(crate) trait AllocatorInteractions {
    async fn register(&self, id: String, port: u16) -> Result<bool, Box<dyn Error>>;
}

pub(crate) struct AllocatorClient {
    client: Arc<Client>,
    address: String,
}

impl Clone for AllocatorClient {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            address: self.address.clone(),
        }
    }
}

impl AllocatorClient {
    pub fn new(client: Arc<Client>, address: String) -> Self {
        Self {
            client,
            address,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct RegisterRequest {
    id: String,
    port: u16
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
}

#[async_trait]
impl AllocatorInteractions for AllocatorClient {
    async fn register(&self, id: String, port: u16) -> Result<bool, Box<dyn Error>> {
        let request_url = format!("{}/register", self.address);
        let resp = self.client
            .post(request_url)
            .json(&RegisterRequest { id, port })
            .send().await?;
        let data: GenericResponse = resp.json().await?;
        Ok(data.is_success())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use reqwest::Client;
    use serde_json::json;
    use crate::clients::allocator_client::{AllocatorClient, AllocatorInteractions};

    #[tokio::test]
    async fn test_register_call_success() {
        let server = MockServer::start();

        let registration_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/register");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Successfully registered"}));
        });

        let client = Arc::new(Client::new());
        let allocator = AllocatorClient {
            client,
            address: server.url(""),
        };
        assert_eq!(true, allocator.register("random-id".to_string(), 3000).await.unwrap());
        registration_mock.assert();
    }

    #[tokio::test]
    async fn test_register_call_failure() {
        let server = MockServer::start();

        let registration_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/register");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Successfully registered"}));
        });

        let client = Arc::new(Client::new());
        let allocator = AllocatorClient {
            client,
            address: server.url(""),
        };
        assert_eq!(false, allocator.register("random-id".to_string(), 3000).await.unwrap());
        registration_mock.assert();
    }
}