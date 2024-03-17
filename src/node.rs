use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::{RwLock};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use crate::hash::hash;

struct Node {
    store: RwLock<HashMap<String, String>>,
    key_count: AtomicU32,
}

impl Node {
    fn new() -> Self {
        Self {
            store: RwLock::new(Default::default()),
            key_count: AtomicU32::new(0),
        }
    }

    pub fn insert_key(&self, key: String, val: String) -> bool {
        let mut stored = false;
        let mut store = self.store.write().unwrap();
        if !store.contains_key(&key) {
            stored = true;
            self.key_count.fetch_add(1, AcqRel);
            store.insert(key, val);
        }
        stored
    }

    pub fn total_keys(&self) -> u32 {
        self.key_count.load(Acquire)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let store = self.store.read().unwrap();
        if let Some(str) = store.get(key) {
            return Some(String::from(str));
        }
        None
    }

    pub fn migrate(&self, start: u32, end: u32, n: u32) -> HashMap<String, String> {
        let mut removed_hm: HashMap<String, String> = HashMap::new();
        let mut cnt_remove = 0;
        let mut hashmap = self.store.write().unwrap();
        for (k, v) in hashmap.clone().iter() {
            let hash_val = hash(k, &n);
            if hash_val.clone() >= start && hash_val <= end {
                cnt_remove += 1;
                removed_hm.insert(String::from(k), String::from(v));
                hashmap.remove(k);
            }
        }
        self.key_count.fetch_sub(cnt_remove, AcqRel);
        removed_hm
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::thread::{scope};
    use crate::hash::hash;
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

        scope(|s| {
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

    #[test]
    fn test_multi_thread_duplicate_inserts() {
        let node = Node::new();
        let threads = 10;
        let vals = 100;

        scope(|s| {
            for mut thread_idx in 0..threads {
                let node = &node;
                s.spawn(move || {
                    for i in 0..vals {
                        if (thread_idx.clone() % 2 != 0) {
                            thread_idx -= 1;
                        }
                        node.insert_key(format!("{thread_idx}-{i}"), format!("val: {thread_idx}-{i}"));
                    }
                });
            }
        });

        assert_eq!(node.total_keys(), (threads / 2) * vals);
    }

    #[test]
    fn test_single_threaded_gets() {
        let mut node = Node::new();
        assert_eq!(true, node.insert_key("foo".to_string(), "bar".to_string()));
        assert_eq!(1, node.total_keys());
        assert_eq!(true, node.insert_key("foo1".to_string(), "bar1".to_string()));
        assert_eq!(2, node.total_keys());
        assert_eq!(None, node.get(&"fooo".to_string()));
        assert_eq!(node.get(&"foo".to_string()), Some("bar".to_string()));
    }

    #[test]
    fn test_single_threaded_migrate() {
        let mut node = Node::new();
        let n = 5;
        let mut expected_hm: HashMap<String, String> = HashMap::new();
        let start = 1u32;
        let end = 3u32;

        for i in 1..10 {
            let k = format!("k-{i}");
            let v = format!("v-{i}");
            let h = hash(&k, &n);
            if h >= start && h <= end {
                &expected_hm.insert(k.clone(), v.clone());
            }
            &node.insert_key(k, v);
        }
        assert_eq!(expected_hm, node.migrate(start, end, n));
        assert_eq!(HashMap::new(), node.migrate(start, end, n));
    }
}
