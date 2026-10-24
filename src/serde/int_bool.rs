use serde::de;
use serde::de::Visitor;
use serde::{Deserializer, Serializer};
use std::fmt::Formatter;

/// Serialize a boolean as an integer (0 or 1).
pub fn serialize<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(i64::from(*value))
}

/// Deserialize an integer (0 or 1) as a boolean.
pub fn deserialize<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolVisitor;
    impl<'de> Visitor<'de> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "expected an integer representing a boolean value")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(de::Error::custom("invalid boolean value")),
            }
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(de::Error::custom("invalid boolean value")),
            }
        }
    }

    deserializer.deserialize_any(BoolVisitor {})
}

#[cfg(test)]
mod tests {
    use crate::bencode;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "crate::serde::int_bool")]
        flag: bool,
    }

    #[test]
    fn test_serialize_true() {
        let result = bencode::to_bytes(&Wrapper { flag: true }).unwrap();

        assert_eq!(b"d4:flagi1ee".to_vec(), result);
    }

    #[test]
    fn test_serialize_false() {
        let result = bencode::to_bytes(&Wrapper { flag: false }).unwrap();

        assert_eq!(b"d4:flagi0ee".to_vec(), result);
    }

    #[test]
    fn test_deserialize_zero_and_one() {
        let result: Wrapper = bencode::from_bytes(b"d4:flagi1ee").unwrap();
        assert_eq!(Wrapper { flag: true }, result);

        let result: Wrapper = bencode::from_bytes(b"d4:flagi0ee").unwrap();
        assert_eq!(Wrapper { flag: false }, result);
    }

    #[test]
    fn test_deserialize_out_of_range() {
        for data in [&b"d4:flagi2ee"[..], b"d4:flagi-1ee", b"d4:flagi100ee"] {
            let result = bencode::from_bytes::<Wrapper>(data);

            assert!(result.is_err(), "expected an error for {:?}", data);
        }
    }

    #[test]
    fn test_deserialize_wrong_type() {
        let result = bencode::from_bytes::<Wrapper>(b"d4:flag1:1e");

        assert!(result.is_err(), "expected an error, got {:?}", result);
    }

    #[test]
    fn test_round_trip() {
        for flag in [true, false] {
            let bytes = bencode::to_bytes(&Wrapper { flag }).unwrap();
            let result: Wrapper = bencode::from_bytes(&bytes).unwrap();

            assert_eq!(Wrapper { flag }, result);
        }
    }
}
