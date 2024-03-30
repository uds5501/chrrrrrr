use ahash::AHasher;
use std::hash::Hasher;

pub fn hash(str: &String, n: &u32) -> u32 {
    let mut hasher = AHasher::default();
    hasher.write(str.as_bytes());
    let result = hasher.finish() as u32;
    result % n
}

#[cfg(test)]
mod tests {
    use crate::core::hash::hash;

    #[test]
    fn test_deterministic_hash() {
        let string1 = "foo".to_string();
        let string2 = "foo".to_string();
        let n = 5;
        assert_eq!(hash(&string1, &n), hash(&string2, &n));
    }
}
