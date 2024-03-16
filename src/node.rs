use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{AcqRel, Acquire};


struct Node {
    store: Mutex<HashMap<String, String>>,
    key_count: AtomicU32,
}

impl Node {
    fn new() -> Self {
        Self {
            store: Mutex::new(Default::default()),
            key_count: AtomicU32::new(0),
        }
    }

    pub fn insert_key(&self, key: String, val: String) -> bool {
        let mut stored = false;
        let mut store = self.store.lock().unwrap();
        if !store.contains_key(&key) {
            stored = true;
            self.key_count.fetch_add(1, AcqRel);
            store.insert(key, val);
        }
        stored
    }

    // This doesn't require mutex.
    pub fn total_keys(&self) -> u32 {
        self.key_count.load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::{scope, spawn};
    use crate::node::Node;

    #[test]
    fn test_single_threaded_inserts() {
        let mut node = Node::new();
        assert_eq!(true, node.insert_key("foo".to_string(), "bar".to_string()));
        assert_eq!(1, node.total_keys());
        assert_eq!(true, node.insert_key("foo1".to_string(), "bar1".to_string()));
        assert_eq!(2, node.total_keys());
        assert_eq!(false, node.insert_key("foo1".to_string(), "bar1".to_string()));
        assert_eq!(2, node.total_keys());
    }

    #[test]
    fn test_multi_thread_inserts() {
        let node = Node::new();
        let threads = 10;
        let vals = 100;

        std::thread::scope(|s| {
            for thread_idx in 0..threads {
                let node = &node;
                s.spawn(move || {
                    for i in 0..vals {
                        node.insert_key(format!("{thread_idx}-{i}"), format!("val: {thread_idx}-{i}"));
                    }
                });
            }
        });

        assert_eq!(node.total_keys(), threads * vals);
    }
}