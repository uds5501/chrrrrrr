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
        self.node_from_idx(self.identify_nearest_slot_anti_clockwise(hash))
    }

    pub fn node_from_idx(&self, slot: u32) -> Option<&NodeClient> {
        self.slot_node_map.get(&slot)
    }

    pub fn identify_nearest_slot_anti_clockwise(&self, hash: u32) -> u32 {
        let idx = self.identify_nearest_idx_anti_clockwise(hash);
        if idx == u32::MAX as usize {
            return idx as u32;
        }
        let val = self.acquired_slots[idx];
        debug!("Slot array: {:?}, returned value - {}", self.acquired_slots, val);
        val
    }

    pub fn identify_nearest_idx_anti_clockwise(&self, hash: u32) -> usize {
        let mut low = 0;
        let mut high = self.acquired_slots.len();
        if high == 0 {
            return u32::MAX as usize;
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
            debug!("Slot array: {:?}, returned index - {}", self.acquired_slots, self.acquired_slots.len() - 1);
            return self.acquired_slots.len() - 1;
        }
        debug!("Slot array: {:?}, returned index - {}", self.acquired_slots, low);
        low
    }

    pub fn get_ranges_to_discard(&self, old_node_slot: u32, new_node_slot: u32) -> Vec<(u32, u32)> {
        let mut result: Vec<(u32, u32)> = vec![];
        let old_node_idx = self.identify_nearest_idx_anti_clockwise(old_node_slot);

        // if it's the last index of the array, push [0, first_elem - 1], [new, end]
        if old_node_idx == self.acquired_slots.len() - 1 {
            if self.acquired_slots[0] != 0 {
                result.push((0, self.acquired_slots[0] - 1));
            }
            result.push((new_node_slot, self.total_slots - 1));
        } else {
            result.push((new_node_slot, self.acquired_slots[old_node_idx + 1] - 1));
        }

        result
    }
    pub fn get_ranges_to_discard_on_deregistration(&self, old_node_slot: u32) -> Vec<(u32, u32)> {
        let mut result: Vec<(u32, u32)> = vec![];
        if self.acquired_slots.len() == 1 {
            return result;
        }
        let old_node_idx = self.identify_nearest_idx_anti_clockwise(old_node_slot);

        // if it's the last index of the array, push [0, first_elem - 1], [new, end]
        if old_node_idx == self.acquired_slots.len() - 1 {
            result.push((old_node_slot, self.total_slots - 1));
        } else {
            result.push((old_node_slot, self.acquired_slots[old_node_idx + 1] - 1));
        }
        result
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
    async fn healthcheck(&self) -> Result<bool, Box<dyn Error>>;
}

#[async_trait]
impl StorageInteractions for Allocator {
    async fn store(&self, key: &String, val: &String) -> Result<bool, Box<dyn Error>> {
        let mut inserted = false;
        if self.nodes.load(Acquire) == 0 {
            return Ok(inserted);
        }
        debug!("kv hash -> {}", hash(key, &self.slots));
        if let Some(node) = self.metadata.read().await.node_for_hash(hash(key, &self.slots)) {
            debug!("node assigned -> {}", node.get_id());
            match node.insert_key(key.clone(), val.clone()).await {
                Ok(ins) => { inserted = ins }
                Err(e) => { return Err(e); }
            }
        }
        Ok(inserted)
    }

    async fn get(&self, key: &String) -> Result<String, Box<dyn Error>> {
        let mut res = String::from("");
        if self.nodes.load(Acquire) == 0 {
            return Ok(res);
        }
        debug!("kv hash -> {}", hash(key, &self.slots));
        if let Some(node) = self.metadata.read().await.node_for_hash(hash(key, &self.slots)) {
            debug!("node assigned -> {}", node.get_id());
            res = node.get(key.clone()).await?
        }
        Ok(res)
    }
}

#[async_trait]
pub trait RegistrationInternals {
    async fn insert_and_migrate(&self, node: NodeClient) -> Result<bool, Box<dyn Error>>;
    async fn deregister_and_migrate(&self, node: &NodeClient) -> Result<bool, Box<dyn Error>>;
}

#[async_trait]
impl RegistrationInternals for Allocator {
    async fn insert_and_migrate(&self, node: NodeClient) -> Result<bool, Box<dyn Error>> {
        let mut metadata = self.metadata.write().await;
        let new_node_slot = hash(&node.get_id(), &metadata.total_slots);
        let old_node_slot = metadata.identify_nearest_slot_anti_clockwise(new_node_slot);

        // if it's not the first element.
        if metadata.acquired_slots.len() != 0 {
            let old_node = metadata.slot_node_map.get(&old_node_slot).unwrap();
            let ranges_to_discard = metadata.get_ranges_to_discard(old_node_slot, new_node_slot);
            for (start, end) in ranges_to_discard {
                debug!("Migrating keys between [{start} - {end}]");
                // this could be done in background later on.
                match old_node.fetch_migrated(start, end, metadata.total_slots).await {
                    Ok(kvs) => {
                        if let Some(removed_kvs) = kvs {
                            for (key, value) in removed_kvs.iter() {
                                debug!("[migration] attempting migration of {}:{} to node : {}", &key, &value, &node.get_id());
                                if !node.insert_key(key.clone(), value.clone()).await? {
                                    debug!("[migration] insertion of {}:{} failed in node : {}", &key, &value, &node.get_id())
                                } else {
                                    debug!("[migration] insertion of {}:{} successful for node : {}", &key, &value, &node.get_id())
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Some error occurred when fetching migration keys");
                        return Err(e);
                    }
                }
            }
        }

        self.nodes.fetch_add(1, AcqRel);
        metadata.acquired_slots.push(new_node_slot);
        metadata.acquired_slots.sort();
        debug!("Slot array after node addition : {:?}", metadata.acquired_slots);
        metadata.slot_node_map.insert(new_node_slot, node);
        debug!("slot node map after node addition : {:?}", metadata.slot_node_map);
        Ok(true)
    }

    async fn deregister_and_migrate(&self, node: &NodeClient) -> Result<bool, Box<dyn Error>> {
        debug!("de-registering {}", node.get_id());
        let mut metadata = self.metadata.write().await;
        let slot_to_remove = hash(&node.get_id(), &metadata.total_slots);

        let old_node_idx = metadata.identify_nearest_idx_anti_clockwise(slot_to_remove);
        let new_node_idx = &((old_node_idx - 1) as u32 % metadata.total_slots);
        // if it's not the only element.
        if metadata.acquired_slots.len() != 0 {
            let new_node = metadata.slot_node_map.get(&metadata.acquired_slots[*new_node_idx as usize]).unwrap();
            let ranges_to_discard = metadata.get_ranges_to_discard_on_deregistration(slot_to_remove);
            for (start, end) in ranges_to_discard {
                debug!("Migrating keys between [{start} - {end}]");
                // this could be done in background later on.
                match node.fetch_migrated(start, end, metadata.total_slots).await {
                    Ok(kvs) => {
                        if let Some(removed_kvs) = kvs {
                            for (key, value) in removed_kvs.iter() {
                                debug!("[migration] attempting migration of {}:{} to node : {}", &key, &value, &node.get_id());
                                if !new_node.insert_key(key.clone(), value.clone()).await? {
                                    debug!("[migration] insertion of {}:{} failed in node : {}", &key, &value, &new_node.get_id())
                                } else {
                                    debug!("[migration] insertion of {}:{} successful for node : {}", &key, &value, &new_node.get_id())
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Some error occurred when fetching migration keys");
                        return Err(e);
                    }
                }
            }
        }

        self.nodes.fetch_sub(1, AcqRel);
        metadata.acquired_slots.remove(old_node_idx);
        debug!("Slot array after node addition : {:?}", metadata.acquired_slots);
        metadata.slot_node_map.remove(&slot_to_remove);
        debug!("slot node map after node addition : {:?}", metadata.slot_node_map);
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

    async fn healthcheck(&self) -> Result<bool, Box<dyn Error>> {
        let nodes_to_deregister = {
            let metadata = self.metadata.read().await;
            let mut nodes = vec![];
            for (slot, node) in metadata.slot_node_map.iter() {
                debug!("Performing health check for {slot}");
                if !node.health_check().await.unwrap() {
                    nodes.push(node.clone());
                }
            }
            nodes
        };

        debug!("Slots to deregister --> {:?}", nodes_to_deregister);

        for node in nodes_to_deregister {
            self.deregister_and_migrate(&node).await?;
        }

        Ok(true)
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
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use serde_json::json;
    use tokio::sync::Semaphore;
    use crate::clients::node_client::NodeClient;
    use crate::core::allocator::{AllocationMetadata, Allocator, RegistrationInteractions};
    use crate::core::hash::hash;

    #[test]
    pub fn test_lower_bound_when_vec_is_empty() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![];
        assert_eq!(u32::MAX, metadata.identify_nearest_slot_anti_clockwise(1));
        assert_eq!(u32::MAX, metadata.identify_nearest_slot_anti_clockwise(8));
        assert_eq!(u32::MAX, metadata.identify_nearest_slot_anti_clockwise(21));
        assert_eq!(u32::MAX, metadata.identify_nearest_slot_anti_clockwise(29));
    }

    #[test]
    pub fn test_lower_bound_impl() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![1, 3, 7, 9, 11, 18, 24];
        assert_eq!(3, metadata.identify_nearest_slot_anti_clockwise(6));
        assert_eq!(9, metadata.identify_nearest_slot_anti_clockwise(9));
        assert_eq!(24, metadata.identify_nearest_slot_anti_clockwise(0));
        assert_eq!(18, metadata.identify_nearest_slot_anti_clockwise(20));
        assert_eq!(24, metadata.identify_nearest_slot_anti_clockwise(29));
    }

    #[test]
    pub fn test_lower_bound_impl_for_single_node() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![1];
        assert_eq!(1, metadata.identify_nearest_slot_anti_clockwise(6));
        assert_eq!(1, metadata.identify_nearest_slot_anti_clockwise(9));
        assert_eq!(1, metadata.identify_nearest_slot_anti_clockwise(0));
        assert_eq!(1, metadata.identify_nearest_slot_anti_clockwise(20));
    }

    #[test]
    pub fn test_migration_ranges_for_single_node() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![4];
        assert_eq!(vec![(0, 3), (6, 29)], metadata.get_ranges_to_discard(4, 6));
        assert_eq!(vec![(0, 3), (5, 29)], metadata.get_ranges_to_discard(4, 5));

        metadata.acquired_slots = vec![0];
        assert_eq!(vec![(5, 29)], metadata.get_ranges_to_discard(0, 5));
        assert_eq!(vec![(10, 29)], metadata.get_ranges_to_discard(0, 10));
    }

    #[test]
    pub fn test_migration_ranges_for_multi_nodes() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![4, 9, 13];
        assert_eq!(vec![(12, 12)], metadata.get_ranges_to_discard(9, 12));
        assert_eq!(vec![(10, 12)], metadata.get_ranges_to_discard(9, 10));
        assert_eq!(vec![(11, 12)], metadata.get_ranges_to_discard(9, 11));


        assert_eq!(vec![(6, 8)], metadata.get_ranges_to_discard(4, 6));

        assert_eq!(vec![(0, 3), (21, 29)], metadata.get_ranges_to_discard(13, 21));
        assert_eq!(vec![(0, 3), (14, 29)], metadata.get_ranges_to_discard(13, 14));
    }

    #[test]
    pub fn test_migration_ranges_on_deregister() {
        let mut metadata = AllocationMetadata::new(30);
        metadata.acquired_slots = vec![4, 9, 13];
        assert_eq!(vec![(9, 12)], metadata.get_ranges_to_discard_on_deregistration(9));
        assert_eq!(vec![(4, 8)], metadata.get_ranges_to_discard_on_deregistration(4));
        assert_eq!(vec![(13, 29)], metadata.get_ranges_to_discard_on_deregistration(13));

        metadata.acquired_slots = vec![10];
        assert_eq!(0, metadata.get_ranges_to_discard_on_deregistration(10).len());
    }

    #[tokio::test]
    async fn test_node_insertion_when_fresh_setup() {
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());
        let id = "random-id".to_string();
        let node = NodeClient::new(client, "".to_string(), id.clone());
        let expected_hash = hash(&id, &10);

        assert_eq!(true, allocator.register_node(node).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![expected_hash], allocator.get_acquired_slots().await.unwrap());
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
        let node1 = NodeClient::new(client.clone(), server.url(""), id1.clone());

        let id2 = "random-id-new".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2.clone());

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![hash(&id1, &10)], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());
        let migrate_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Migration passed"}));
        });
        assert_eq!(true, allocator.register_node(node2).await.unwrap());
        let mut expected_vec = vec![hash(&id1, &10), hash(&id2, &10)];
        expected_vec.sort();
        assert_eq!(expected_vec, allocator.get_acquired_slots().await.unwrap());
        assert_eq!(2, allocator.get_slot_node_map().await.unwrap().len());
        migrate_mock.assert_hits(2);
    }

    #[tokio::test]
    async fn test_sequential_node_insertion_with_migration() {
        let server = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "random-id".to_string();
        let node1 = NodeClient::new(client.clone(), server.url(""), id1.clone());

        let id2 = "random-id-new".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2.clone());

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![hash(&id1, &10)], allocator.get_acquired_slots().await.unwrap());
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
        let mut expected_vec = vec![hash(&id1, &10), hash(&id2, &10)];
        expected_vec.sort();
        assert_eq!(expected_vec, allocator.get_acquired_slots().await.unwrap());
        assert_eq!(2, allocator.get_slot_node_map().await.unwrap().len());
    }

    #[tokio::test]
    async fn test_sequential_node_insertion_should_fail_on_conflicting_nodes() {
        let server = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "random-id".to_string();
        let node1 = NodeClient::new(client.clone(), server.url(""), id1.clone());

        let id2 = "random-id".to_string();
        let node2 = NodeClient::new(client.clone(), server.url(""), id2.clone());

        assert_eq!(true, allocator.register_node(node1).await.unwrap());
        assert_eq!(10, allocator.get_total_nodes().await.unwrap());
        assert_eq!(vec![hash(&id1, &10)], allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());


        assert_eq!(false, allocator.register_node(node2).await.unwrap());
        assert_eq!(vec![hash(&id1, &10)], allocator.get_acquired_slots().await.unwrap());
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
    }

    #[tokio::test]
    async fn test_deregisteration_successfully() {
        let server1 = MockServer::start();
        let server2 = MockServer::start();
        let allocator = Allocator::new(10);
        let client = Arc::new(reqwest::Client::new());

        let id1 = "adakahdkahdi2hei3k13bk1".to_string();
        let h1 = hash(&id1, &10);
        let node1 = NodeClient::new(client.clone(), server1.url(""), id1.clone());

        let id2 = "fakakdhak2312k3h1kehkd".to_string();
        let h2 = hash(&id2, &10);
        let node2 = NodeClient::new(client.clone(), server2.url(""), id2.clone());

        {
            let mut metadata = allocator.metadata.write().await;
            metadata.acquired_slots = vec![h1.clone(), h2.clone()];
            metadata.slot_node_map.insert(h1, node1);
            metadata.slot_node_map.insert(h2, node2);
        }

        let health_mock_1 = server1.mock(|when, then| {
            when.method(GET)
                .path("/health");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "Healthy"}));
        });
        let health_mock_2 = server2.mock(|when, then| {
            when.method(GET)
                .path("/health");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": false, "message": "Unhealthy"}));
        });
        let migrate_mock = server2.mock(|when, then| {
            when.method(POST)
                .path("/migrate");
            then.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "migration passed", "data": {
                    "k1" : "v1",
                    "k2": "v2"
                }}));
        });
        let insert_mock = server1.mock(|w, t| {
            w.method(POST)
                .path("/insert");
            t.status(200)
                .header("content-type", "JSON")
                .json_body(json!({"success": true, "message": "insertion success"}));
        });

        assert_eq!(true, allocator.healthcheck().await.unwrap());

        let expected_vec = vec![h1];
        assert_eq!(expected_vec, allocator.get_acquired_slots().await.unwrap());
        assert_eq!(1, allocator.get_slot_node_map().await.unwrap().len());

        migrate_mock.assert();
        health_mock_1.assert();
        health_mock_2.assert();
        insert_mock.assert_hits(2);
    }
}

