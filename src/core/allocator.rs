use std::collections::HashMap;
use std::error::Error;

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use async_trait::async_trait;
use log::debug;
use crate::core::hash::hash;
use crate::clients::node_client::{NodeInteractions, NodeClient};
use tokio::sync::RwLock;


struct AllocationMetadata {
    total_slots: u32,
    acquired_slots: Vec<u32>,
    slot_node_map: HashMap<u32, NodeClient>,
}

pub struct Allocator {
    slots: u32,
    nodes: AtomicU32,
    metadata: RwLock<AllocationMetadata>,
}

impl AllocationMetadata {
    pub fn new(slots: u32) -> Self {
        Self { total_slots: slots, acquired_slots: vec![], slot_node_map: Default::default() }
    }

    pub fn node_for_hash(&self, hash: u32) -> Option<&NodeClient> {
        self.node_from_idx(self.identify_idx_for_slot(hash))
    }

    pub fn node_from_idx(&self, slot: u32) -> Option<&NodeClient> {
        self.slot_node_map.get(&slot)
    }

    pub fn identify_idx_for_slot(&self, hash: u32) -> u32 {
        let mut low = 0;
        let mut high = self.acquired_slots.len();
        if high == 0 {
            return 0;
        }
        while low <= high {
            let mid: usize = (low + high) / 2;
            if self.acquired_slots[mid] <= hash {
                low = mid;
            } else if self.acquired_slots[mid] > hash {
                high = mid
            }
            if high - low <= 1 {
                break;
            }
        }
        if self.acquired_slots[low] > hash {
            return self.acquired_slots[self.acquired_slots.len() - 1];
        }
        self.acquired_slots[low]
    }
}

#[async_trait]
pub trait StorageInteractions {
    async fn store(&self, key: &String, val: &String) -> Result<bool, Box<dyn Error>>;
    async fn get(&self, key: &String) -> Result<String, Box<dyn Error>>;
}

#[async_trait]
pub trait RegistrationInteractions {
    async fn register_node(&self, node: NodeClient) -> Result<bool, Box<dyn Error>>;
    async fn de_register_node(&self, node: NodeClient) -> Result<bool, Box<dyn Error>>;
    async fn get_acquired_slots(&self) -> Result<Vec<u32>, Box<dyn Error>>;
    async fn get_total_nodes(&self) -> Result<u32, Box<dyn Error>>;
    async fn get_slot_node_map(&self) -> Result<HashMap<u32, NodeClient>, Box<dyn Error>>;
}

#[async_trait]
impl StorageInteractions for Allocator {
    async fn store(&self, key: &String, val: &String) -> Result<bool, Box<dyn Error>> {
        let mut inserted = false;
        if self.nodes.load(Acquire) == 0 {
            return Ok(inserted);
        }
        if let Some(node) = self.metadata.read().await.node_for_hash(hash(key, &self.slots)) {
            match node.insert_key(key.clone(), val.clone()).await {
                Ok(ins) => { inserted = ins }
                Err(e) => { return Err(e); }
            }
            inserted = node.insert_key(key.clone(), val.clone()).await.unwrap();
        }
        Ok(inserted)
    }

    async fn get(&self, key: &String) -> Result<String, Box<dyn Error>> {
        let mut res = String::from("");
        if self.nodes.load(Acquire) == 0 {
            return Ok(res);
        }
        if let Some(node) = self.metadata.read().await.node_for_hash(hash(key, &self.slots)) {
            res = node.get(key.clone()).await?
        }
        Ok(res)
    }
}

#[async_trait]
pub trait RegistrationInternals {
    async fn insert_and_migrate(&self, node: NodeClient) -> Result<bool, Box<dyn Error>>;
}

#[async_trait]
impl RegistrationInternals for Allocator {
    async fn insert_and_migrate(&self, node: NodeClient) -> Result<bool, Box<dyn Error>> {
        let mut metadata = self.metadata.write().await;
        let slot = hash(&node.get_id(), &metadata.total_slots);
        let idx = metadata.identify_idx_for_slot(slot);

        // if it's not the first element.
        if metadata.acquired_slots.len() != 0 {
            let len = metadata.acquired_slots.len();
            let last_node_idx = metadata.acquired_slots[idx as usize % len];
            let old_node = metadata.slot_node_map.get(&last_node_idx).unwrap();
            let old_node_end_idx = metadata.identify_idx_for_slot(last_node_idx + 1);

            // this could be done in background later on.
            match old_node.fetch_migrated(slot, old_node_end_idx, metadata.total_slots).await {
                Ok(kvs) => {
                    if let Some(removed_kvs) = kvs {
                        for (key, value) in removed_kvs.iter() {
                            if !node.insert_key(key.clone(), value.clone()).await? {
                                debug!("[migration] insertion of {}:{} failed in node : {}", &key, &value, &node.get_id())
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        self.nodes.fetch_add(1, AcqRel);
        metadata.acquired_slots.push(slot);
        metadata.acquired_slots.sort();
        metadata.slot_node_map.insert(slot, node);
        Ok(true)
    }
}

#[async_trait]
impl RegistrationInteractions for Allocator {
    async fn register_node(&self, node: NodeClient) -> Result<bool, Box<dyn Error>> {
        {
            let metadata = self.metadata.read().await;
            if metadata.total_slots == metadata.acquired_slots.len() as u32 {
                return Ok(false);
            }
            let slot = hash(&node.get_id(), &metadata.total_slots);
            if metadata.slot_node_map.contains_key(&slot) {
                println!("New node {:?} can't be added as slot {slot} is already taken", node.get_id());
                return Ok(false);
            }
        }
        let inserted = self.insert_and_migrate(node).await?;
        Ok(inserted)
    }
    async fn de_register_node(&self, _node: NodeClient) -> Result<bool, Box<dyn Error>> {
        Ok(true)
    }

    async fn get_acquired_slots(&self) -> Result<Vec<u32>, Box<dyn Error>> {
        Ok(self.metadata.read().await.acquired_slots.clone())
    }

    async fn get_total_nodes(&self) -> Result<u32, Box<dyn Error>> {
        Ok(self.metadata.read().await.total_slots.clone())
    }

    async fn get_slot_node_map(&self) -> Result<HashMap<u32, NodeClient>, Box<dyn Error>> {
        Ok(self.metadata.read().await.slot_node_map.clone())
    }
}

impl Allocator {
    pub fn new(slots: u32) -> Self {
        Self {
            slots,
            nodes: AtomicU32::new(0),
            metadata: RwLock::new(AllocationMetadata::new(slots)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use serde_json::json;
    use tokio::sync::Semaphore;
    use crate::clients::node_client::NodeClient;
    use crate::core::allocator::{AllocationMetadata, Allocator, RegistrationInteractions};

    #[test]
    pub fn test_lower_bound_when_vec_is_empty() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![];
        assert_eq!(0, metadata.identify_idx_for_slot(1));
        assert_eq!(0, metadata.identify_idx_for_slot(8));
        assert_eq!(0, metadata.identify_idx_for_slot(21));
        assert_eq!(0, metadata.identify_idx_for_slot(29));
    }

    #[test]
    pub fn test_lower_bound_impl() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![1, 3, 7, 9, 11, 18, 24];
        assert_eq!(3, metadata.identify_idx_for_slot(6));
        assert_eq!(9, metadata.identify_idx_for_slot(9));
        assert_eq!(24, metadata.identify_idx_for_slot(0));
        assert_eq!(18, metadata.identify_idx_for_slot(20));
        assert_eq!(24, metadata.identify_idx_for_slot(29));
    }

    #[test]
    pub fn test_lower_bound_impl_for_single_node() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![1];
        assert_eq!(1, metadata.identify_idx_for_slot(6));
        assert_eq!(1, metadata.identify_idx_for_slot(9));
        assert_eq!(1, metadata.identify_idx_for_slot(0));
        assert_eq!(1, metadata.identify_idx_for_slot(20));
    }

    #[tokio::test]
    async fn test_node_insertion_when_fresh_setup() {
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());
        let id = "random-id".to_string();
        let node = NodeClient::new(client, "".to_string(), id);

        assert_eq!(true, allocator.register_node(node).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());
    }

    #[tokio::test]
    async fn test_node_insertion_when_at_capacity() {
        let allocator = Allocator::new(0);
        let client = Arc::new(reqwest::Client::new());
        let id = "random-id".to_string();
        let node = NodeClient::new(client, "".to_string(), id);

        assert_eq!(false, allocator.register_node(node).await.unwrap());
        assert_eq!(0, allocator.get_total_nodes().await.unwrap());
        assert_eq!(0, allocator.get_slot_node_map().await.unwrap().len());
    }

    #[tokio::test]
    async fn test_sequential_node_insertion() {
        let server = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "random-id".to_string();
        let node1 = NodeClient::new(client.clone(), server.url(""), id1);

        let id2 = "random-id-new".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2);

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());
        let migrate_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Migration passed"}));
        });
        assert_eq!(true, allocator.register_node(node2).await.unwrap());
        assert_eq!(vec![3, 6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(2, allocator.get_slot_node_map().await.unwrap().len());
        migrate_mock.assert();
    }

    #[tokio::test]
    async fn test_sequential_node_insertion_with_migration() {
        let server = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "random-id".to_string();
        let node1 = NodeClient::new(client.clone(), server.url(""), id1);

        let id2 = "random-id-new".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2);

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());
        let migrate_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Migration passed", "data": {
                    "k1" : "v1",
                    "k2" : "v2"
                }}));
        });
        let insert_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/insert");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Insertion Successful"}));
        });
        assert_eq!(true, allocator.register_node(node2).await.unwrap());
        assert_eq!(vec![3, 6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(2, allocator.get_slot_node_map().await.unwrap().len());
        migrate_mock.assert();
        insert_mock.assert_hits(2);
    }

    #[tokio::test]
    async fn test_sequential_node_insertion_should_fail_on_conflicting_nodes() {
        let server = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "random-id".to_string();
        let node1 = NodeClient::new(client.clone(), server.url(""), id1);

        let id2 = "random-id-2".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2);

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());


        assert_eq!(false, allocator.register_node(node2).await.unwrap());
        assert_eq!(vec![6], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());
    }

    #[tokio::test]
    async fn test_simultaneous_node_insertion_with_migration() {
        let server = MockServer::start();
        let allocator = Arc::new(Allocator::new(550));
        let client = Arc::new(reqwest::Client::new());

        let mut nodes = Vec::with_capacity(10);
        for i in 0..10 {
            let id = format!("random-id-{}", i);
            let node = NodeClient::new(client.clone(), server.url(""), id);
            nodes.push(node);
        }

        let migrate_mock = server.mock(|when, then| {
            when.method(POST).path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Migration passed", "data": {
                "k1" : "v1",
                "k2" : "v2"
            }}));
        });

        let insert_mock = server.mock(|when, then| {
            when.method(POST).path("/insert");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Insertion Successful"}));
        });

        const MAX_CONCURRENT_REGISTRATIONS: usize = 5;
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REGISTRATIONS));

        let mut handles = Vec::with_capacity(10);
        for node in nodes {
            let allocator_clone = allocator.clone();
            let semaphore_clone = semaphore.clone();
            let handle = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire().await.unwrap();
                allocator_clone.register_node(node).await.unwrap()
            });
            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await;
        for result in results {
            assert_eq!(true, result.unwrap());
        }

        assert_eq!(550, allocator.get_total_nodes().await.unwrap());
        assert_eq!(10, allocator.get_acquired_slots().await.unwrap().len());
        assert_eq!(10, allocator.get_slot_node_map().await.unwrap().len());

        migrate_mock.assert_hits(9); // Assuming each node insertion triggers a migration
        insert_mock.assert_hits(18); // Assuming each node insertion triggers two insertions
    }
}

