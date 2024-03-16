use std::collections::HashMap;

pub struct Allocator {
    slots: i32,
    nodes: i32,
    nodes_idx: Vec<i32>,
    idx_node_map: HashMap<i32, String>,
}
