use crate::dht::storage::MutableItemProperties;
use crate::dht::{Error, PublicKey, Result};
use crate::Sha1Hash;
use ed25519::SignatureBytes;
use sha1::{Digest, Sha1};

/// Parse the mutable items properties from an incoming message.
///
/// Returns the mutable properties for the incoming message if present, else [None].
pub fn parse_mutable_item_properties(
    sequence_nr: Option<u64>,
    public_key: Option<PublicKey>,
    salt: Option<Vec<u8>>,
    signature: Option<Vec<u8>>,
) -> Result<Option<MutableItemProperties>> {
    let public_key = match public_key {
        None => return Ok(None),
        Some(key) => key,
    };
    let sequence_nr = sequence_nr.ok_or(Error::InvalidSequenceNr)?;
    let signature = signature.ok_or(Error::InvalidSignature).and_then(|bytes| {
        SignatureBytes::try_from(bytes.as_slice()).map_err(|_| Error::InvalidSignature)
    })?;

    Ok(Some(MutableItemProperties {
        sequence_nr,
        public_key,
        signature,
        salt,
    }))
}

/// Try to generate the hash key from the given `public key` and optional `salt`.
///
/// Returns the [Sha1Hash] for the [MutableItemProperties], else an error.   
pub fn generate_mutable_item_key(public_key: &[u8], salt: Option<&[u8]>) -> Result<Sha1Hash> {
    let mut bytes = public_key.to_vec();
    if let Some(salt) = salt.as_ref() {
        bytes.extend_from_slice(salt);
    }

    Sha1Hash::try_from(Sha1::digest(bytes.as_slice())).map_err(|e| Error::Parse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    mod generate_mutable_item_key {
        use super::*;
        use rand::{rng, Rng};

        /// BEP44: test 2 (mutable with salt)
        #[test]
        fn test_mutable_with_salt() {
            let expected_result = Sha1Hash::try_from(
                hex::decode("411eba73b6f087ca51a3795d9c8c938d365e32c1").unwrap(),
            )
            .unwrap();
            let public_key: PublicKey = PublicKey::try_from(
                hex::decode("77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548")
                    .unwrap(),
            )
            .unwrap();
            let salt = b"foobar";

            let result = generate_mutable_item_key(&public_key, Some(salt))
                .expect("expected the value key to be generated");

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_without_salt() {
            let mut public_key: PublicKey = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let expected_result = Sha1Hash::try_from(Sha1::digest(public_key.as_ref())).unwrap();

            let result = generate_mutable_item_key(&public_key, None).unwrap();

            assert_eq!(expected_result, result);
        }

        #[test]
        fn test_with_salt() {
            let mut public_key: PublicKey = PublicKey::default();
            rng().fill_bytes(&mut public_key);
            let salt = b"FooBar".to_vec();
            let expected_result = Sha1Hash::try_from(Sha1::digest(
                [public_key.as_ref(), salt.as_slice()].concat(),
            ))
            .unwrap();

            let result = generate_mutable_item_key(&public_key, Some(salt.as_ref())).unwrap();

            assert_eq!(expected_result, result);
        }
    }
}
