use std::fmt;
use serde::de::StdError;

pub(crate) struct NodeGetError {
    message: String,
}

impl NodeGetError {
    pub fn new(msg: String) -> Self {
        Self {
            message: msg,
        }
    }
}

impl fmt::Display for NodeGetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("Node fetch failed with error {}", self.message)) // user-facing output
    }
}

impl fmt::Debug for NodeGetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ file: {}, line: {} }}", file!(), line!()) // programmer-facing output
    }
}

impl StdError for NodeGetError {
    fn description(&self) -> &str {
        &*self.message
    }
}
