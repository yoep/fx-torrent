use crate::bencode;
use crate::dht::{Error, Result};
use ed25519::signature::{Signer, Verifier};
use ed25519::{ComponentBytes, Signature, SignatureBytes};
use ed25519_dalek::{SigningKey, VerifyingKey};
use log::{debug, trace};
use serde::Serialize;

/// The ed25519 public key format.
pub type PublicKey = ComponentBytes;

/// The ed25519 secret key format.
pub type SecretKey = ComponentBytes;

/// The ed25519 item signature used by the DHT protocol.
///
/// This is a wrapper around an [ed25519] implementation that allows
/// **signing** and **validating** item values.
#[derive(Debug)]
pub struct ItemSignature {}

impl ItemSignature {
    /// Create a new verifier based on the enabled features, see `ed25519-dalek`.
    pub fn new() -> Result<Self> {
        Ok(Self::new_dalek())
    }

    /// Create a new `ed25519_dalek` based verifier.
    pub fn new_dalek() -> Self {
        Self {}
    }

    /// Sign the given value with the secret key.
    ///
    /// Returns the signature and public key of the signed value.
    pub fn sign<V>(
        &self,
        value: &V,
        sequence_nr: &u64,
        salt: Option<&[u8]>,
        secret_key: &SecretKey,
    ) -> Result<(SignatureBytes, PublicKey)>
    where
        V: Serialize,
    {
        let salt = salt.map(|s| s.to_vec());
        let item_signature = SignatureItem {
            salt,
            sequence_nr: *sequence_nr,
            value,
        };
        let item_signature_bytes = bencode::to_bytes(&item_signature)?;

        let signer = SigningKey::from_bytes(secret_key);
        signer
            .try_sign(item_signature_bytes.as_slice())
            .map(|e| (e.to_bytes(), signer.verifying_key().to_bytes()))
            .map_err(|e| Error::InvalidMessage(e.to_string()))
    }

    /// Validate the signature of the given value.
    pub fn verify<V>(
        &self,
        value: &V,
        sequence_nr: &u64,
        public_key: &PublicKey,
        salt: Option<&[u8]>,
        signature: &SignatureBytes,
    ) -> Result<()>
    where
        V: Serialize,
    {
        let signature = Signature::from_bytes(signature);
        let salt = salt.map(|s| s.to_vec());
        let verification_item = SignatureItem {
            salt,
            sequence_nr: *sequence_nr,
            value,
        };
        let verification_item_bytes = bencode::to_bytes(&verification_item)?;

        let key = VerifyingKey::from_bytes(public_key).map_err(|e| {
            trace!("DHT item verifier failed to create key, {}", e);
            Error::InvalidSignature
        })?;
        key.verify(&verification_item_bytes, &signature)
            .map_err(|e| {
                debug!("DHT item verifier validation failed, {}", e);
                Error::InvalidSignature
            })
    }
}

#[derive(Debug, Serialize)]
struct SignatureItem<V>
where
    V: Serialize,
{
    #[serde(skip_serializing_if = "Option::is_none")]
    pub salt: Option<Vec<u8>>,
    #[serde(rename = "seq")]
    pub sequence_nr: u64,
    #[serde(rename = "v")]
    pub value: V,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rng, Rng};

    #[derive(Debug, Clone, Serialize)]
    struct TestValue {
        pub name: String,
    }

    #[test]
    fn test_new() {
        let result = ItemSignature::new();
        assert!(
            result.is_ok(),
            "expected an ItemVerifier, but got {:?}",
            result
        );
    }

    #[test]
    fn test_sign_and_verify_without_salt() {
        let value = TestValue {
            name: "Foo".to_string(),
        };
        let sequence_nr = 2;
        let mut secret_key: SecretKey = SecretKey::default();
        rng().fill_bytes(&mut secret_key);
        let item_signature = ItemSignature::new().unwrap();

        // sign the value with the secret key
        let (signature, public_key) = item_signature
            .sign(&value, &sequence_nr, None, &secret_key)
            .expect("expected the value to have been signed");

        // verify with the correct sequence number
        let result = item_signature.verify(&value, &sequence_nr, &public_key, None, &signature);
        assert_eq!(Ok(()), result, "expected the signature to be valid");

        // verify with an incorrect sequence number
        let result = item_signature.verify(&value, &1, &public_key, None, &signature);
        assert_eq!(
            Err(Error::InvalidSignature),
            result,
            "expected the signature to be invalid"
        );
    }

    #[test]
    fn test_sign_and_verify_with_salt() {
        let value = TestValue {
            name: "LoremIpsumDolor".to_string(),
        };
        let sequence_nr = 2;
        let salt = b"MyRandomSalyKey";
        let mut secret_key = [0u8; 32];
        rng().fill_bytes(&mut secret_key);
        let item_signature = ItemSignature::new().unwrap();

        // sign the value with the secret key
        let (signature, public_key) = item_signature
            .sign(&value, &sequence_nr, Some(salt), &secret_key)
            .expect("expected the value to have been signed");

        // verify with the correct sequence number
        let result =
            item_signature.verify(&value, &sequence_nr, &public_key, Some(salt), &signature);
        assert_eq!(Ok(()), result, "expected the signature to be valid");

        // verify with an incorrect sequence number
        let result = item_signature.verify(&value, &1, &public_key, Some(salt), &signature);
        assert_eq!(
            Err(Error::InvalidSignature),
            result,
            "expected the signature to be invalid"
        );
    }
}
