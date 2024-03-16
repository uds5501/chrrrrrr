use std::hash::{DefaultHasher, Hash, Hasher};

pub fn hash(str: String, n: &i32) -> i32 {
    let mut hasher = DefaultHasher::new();
    str.hash(&mut hasher);
    let result = hasher.finish() as i32;
    result % n
}

#[test]
fn test_deterministic_hash() {
    let string1 = "foo".to_string();
    let string2 = "foo".to_string();
    let n = 5;
    assert_eq!(hash(string1, &n), hash(string2, &n));
}