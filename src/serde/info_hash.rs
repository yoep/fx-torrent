use crate::InfoHash;
use itertools::Itertools;
use serde::de::{SeqAccess, Visitor};
use serde::{de, Deserializer, Serializer};
use std::fmt::Formatter;

pub fn serialize<S>(value: &[InfoHash], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let bytes = value
        .iter()
        .map(|e| e.short_info_hash_bytes().to_vec())
        .concat();
    serializer.serialize_bytes(&bytes)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<InfoHash>, D::Error>
where
    D: Deserializer<'de>,
{
    struct InfoHashVisitor;
    impl<'de> Visitor<'de> for InfoHashVisitor {
        type Value = Vec<InfoHash>;

        fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "expected a sequence of info hash bytes")
        }

        fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let n = bytes.len() / 20;
            let mut info_hashes = Vec::with_capacity(n);
            for chunk in bytes.chunks(20) {
                info_hashes.push(InfoHash::try_from_bytes(chunk).map_err(de::Error::custom)?);
            }
            Ok(info_hashes)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut info_hashes = Vec::new();
            while let Some(e) = seq.next_element::<InfoHash>()? {
                info_hashes.push(e);
            }
            Ok(info_hashes)
        }
    }

    D::deserialize_any(deserializer, InfoHashVisitor {})
}

#[cfg(test)]
mod tests {
    use crate::{bencode, InfoHash};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "crate::serde::info_hash")]
        hashes: Vec<InfoHash>,
    }

    fn v1(byte: u8) -> InfoHash {
        InfoHash::try_from_bytes([byte; 20]).unwrap()
    }

    #[test]
    fn test_serialize_concatenates_hashes() {
        let wrapper = Wrapper {
            hashes: vec![v1(1), v1(2)],
        };

        let result = bencode::to_bytes(&wrapper).unwrap();

        let mut expected = b"d6:hashes40:".to_vec();
        expected.extend_from_slice(&[1u8; 20]);
        expected.extend_from_slice(&[2u8; 20]);
        expected.push(b'e');
        assert_eq!(expected, result);
    }

    #[test]
    fn test_serialize_empty() {
        let wrapper = Wrapper { hashes: vec![] };

        let result = bencode::to_bytes(&wrapper).unwrap();

        assert_eq!(b"d6:hashes0:e".to_vec(), result);
    }

    #[test]
    fn test_serialize_v2_uses_short_hash() {
        let v2 = InfoHash::try_from_bytes([7u8; 32]).unwrap();
        let wrapper = Wrapper { hashes: vec![v2] };

        let result = bencode::to_bytes(&wrapper).unwrap();

        let mut expected = b"d6:hashes20:".to_vec();
        expected.extend_from_slice(&[7u8; 20]);
        expected.push(b'e');
        assert_eq!(expected, result);
    }

    #[test]
    fn test_round_trip_preserves_order() {
        let wrapper = Wrapper {
            hashes: vec![v1(3), v1(1), v1(2)],
        };

        let bytes = bencode::to_bytes(&wrapper).unwrap();
        let result: Wrapper = bencode::from_bytes(&bytes).unwrap();

        assert_eq!(wrapper, result);
    }

    #[test]
    fn test_deserialize_empty_bytes() {
        let result: Wrapper = bencode::from_bytes(b"d6:hashes0:e").unwrap();

        assert!(result.hashes.is_empty());
    }

    #[test]
    fn test_deserialize_list() {
        let mut data = b"d6:hashesl20:".to_vec();
        data.extend_from_slice(&[4u8; 20]);
        data.extend_from_slice(b"20:");
        data.extend_from_slice(&[5u8; 20]);
        data.extend_from_slice(b"ee");

        let result: Wrapper = bencode::from_bytes(&data).unwrap();

        assert_eq!(vec![v1(4), v1(5)], result.hashes);
    }

    #[test]
    fn test_deserialize_invalid_length() {
        let mut data = b"d6:hashes25:".to_vec();
        data.extend_from_slice(&[1u8; 25]);
        data.push(b'e');

        let result = bencode::from_bytes::<Wrapper>(&data);

        assert!(result.is_err(), "expected an error, got {:?}", result);
    }
}
