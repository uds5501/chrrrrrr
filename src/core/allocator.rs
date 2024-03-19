use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Acquire;
use async_trait::async_trait;
use crate::core::hash::hash;
use crate::clients::node_client::{NodeInteractions, NodeMetadata};
use tokio::sync::RwLock;


struct AllocationMetadata {
    total_slots: u32,
    acquired_slots: Vec<u32>,
    slot_node_map: HashMap<u32, NodeMetadata>,
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

    pub fn node_for_hash(&self, hash: u32) -> Option<&NodeMetadata> {
        self.node_from_idx(self.identify_idx_for_slot(hash))
    }

    pub fn node_from_idx(&self, slot: u32) -> Option<&NodeMetadata> {
        self.slot_node_map.get(&slot)
    }

    pub fn identify_idx_for_slot(&self, hash: u32) -> u32 {
        let mut low = 0;
        let mut high = self.acquired_slots.len();
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
trait StorageInteractions {
    async fn store(&self, key: &String, val: &String) -> Result<bool, Box<dyn Error>>;
    async fn get(&self, key: &String) -> Result<String, Box<dyn Error>>;
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
                Ok(ins) => {inserted = ins}
                Err(e) => {return Err(e)}
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
    use std::sync::atomic::Ordering::Release;
    use crate::core::allocator::{AllocationMetadata, Allocator, StorageInteractions};
    use reqwest::Client;
    use crate::clients::node_client::{MockNodeMetadata, NodeMetadata};

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
}

