use std::hash::{DefaultHasher, Hash, Hasher};

pub fn hash(str: &String, n: &u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    str.hash(&mut hasher);
    let result = hasher.finish() as u32;
    result % n
}

#[cfg(test)]
mod tests {
    use crate::hash::hash;

    #[test]
    fn test_deterministic_hash() {
        let string1 = "foo".to_string();
        let string2 = "foo".to_string();
        let n = 5;
        assert_eq!(hash(&string1, &n), hash(&string2, &n));
    }
}
