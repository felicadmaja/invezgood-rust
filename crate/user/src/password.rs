//! Hash password dengan bcrypt sebelum disimpan ke Scylla; verifikasi saat Login.

const DEFAULT_COST: u32 = 12;

pub fn hash_password(password: &str) -> Result<String, String> {
    bcrypt::hash(password, DEFAULT_COST).map_err(|e| e.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    bcrypt::verify(password, hash).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("Windows@11").expect("hash");
        assert!(verify_password("Windows@11", &hash).expect("verify"));
        assert!(!verify_password("wrong", &hash).expect("verify wrong"));
    }
}
