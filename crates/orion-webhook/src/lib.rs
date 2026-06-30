//! HTTP webhook signature verification + payload helpers.
//!
//! Pure logic so it's testable without a server. The binary in
//! `src/main.rs` wires this into an axum app.

use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;

/// Verify a GitHub-style HMAC-SHA256 signature header.
///
/// Header form: `sha256=<hex digest>`. Matches what GitHub, GitLab,
/// Slack (after stripping their prefix), and most CI systems send.
pub fn verify_hmac_sha256(secret: &str, body: &[u8], header_value: &str) -> bool {
    let expected = match header_value.strip_prefix("sha256=") {
        Some(s) => s,
        None => header_value, // accept bare hex too
    };
    let expected_bytes = match hex::decode(expected) {
        Ok(b) => b,
        Err(_) => return false,
    };
    let mut mac = match Hmac::<Sha256>::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    mac.verify_slice(&expected_bytes).is_ok()
}

/// What the webhook receiver publishes to the queue.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WebhookEvent {
    pub at: String,
    pub headers: serde_json::Value,
    pub body: serde_json::Value,
    pub raw_body: String,
    pub _subject: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn verify_accepts_correct_signature_with_sha256_prefix() {
        let body = b"payload to sign";
        let sig = format!("sha256={}", sign("supersecret", body));
        assert!(verify_hmac_sha256("supersecret", body, &sig));
    }

    #[test]
    fn verify_accepts_bare_hex_signature() {
        let body = b"another payload";
        let sig = sign("k", body);
        assert!(verify_hmac_sha256("k", body, &sig));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let body = b"x";
        let sig = format!("sha256={}", sign("right", body));
        assert!(!verify_hmac_sha256("wrong", body, &sig));
    }

    #[test]
    fn verify_rejects_wrong_body() {
        let sig = format!("sha256={}", sign("s", b"original"));
        assert!(!verify_hmac_sha256("s", b"tampered", &sig));
    }

    #[test]
    fn verify_rejects_malformed_signature() {
        assert!(!verify_hmac_sha256("s", b"x", "sha256=not-hex"));
        assert!(!verify_hmac_sha256("s", b"x", ""));
    }

    #[test]
    fn verify_is_constant_time_for_equal_length() {
        // Mostly a smoke test — HMAC's verify_slice is constant-time.
        // Confirms two different valid-looking-but-wrong digests both reject.
        let sig1 = "sha256=00000000000000000000000000000000000000000000000000000000ffffffff";
        let sig2 = "sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000";
        assert!(!verify_hmac_sha256("s", b"x", sig1));
        assert!(!verify_hmac_sha256("s", b"x", sig2));
    }
}
