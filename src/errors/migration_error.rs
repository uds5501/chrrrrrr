use std::fmt;
use serde::de::StdError;

pub(crate) struct MigrationError {
    message: String,
}

impl MigrationError {
    pub fn new(msg: String) -> Self {
        Self {
            message: msg,
        }
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", format!("Key migration failed with error: {}", self.message)) // user-facing output
    }
}

impl fmt::Debug for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ file: {}, line: {} }}", file!(), line!()) // programmer-facing output
    }
}

impl StdError for MigrationError {
    fn description(&self) -> &str {
        &*self.message
    }
}