use std::fmt;

pub(crate) struct EmptyNodeListError;

impl fmt::Display for EmptyNodeListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "No nodes present") // user-facing output
    }
}

impl fmt::Debug for EmptyNodeListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{ file: {}, line: {} }}", file!(), line!()) // programmer-facing output
    }
}

fn produce_error() -> Result<(), EmptyNodeListError> {
    Err(EmptyNodeListError)
}
