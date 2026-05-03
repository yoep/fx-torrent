use crate::bencode::{Error, Result};
use serde::de::{Error as _, IntoDeserializer, Unexpected};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{de, forward_to_deserialize_any, Deserializer, Serialize, Serializer};
use serde_bytes::{ByteBuf, Bytes};
use std::collections::HashMap;
use std::{fmt, result};

/// A bencode value that can represent any type.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Value {
    /// A generic list of bytes.
    Bytes(Vec<u8>),
    /// An integer.
    Int(i64),
    /// A list of other bencoded values.
    List(Vec<Value>),
    /// A map of (key, value) pairs.
    Dict(HashMap<Vec<u8>, Value>),
}

impl Serialize for Value {
    #[inline]
    fn serialize<S>(&self, s: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Bytes(ref v) => s.serialize_bytes(v),
            Value::Int(v) => s.serialize_i64(v),
            Value::List(ref v) => {
                let mut seq = s.serialize_seq(Some(v.len()))?;
                for e in v {
                    seq.serialize_element(e)?;
                }
                seq.end()
            }
            Value::Dict(ref vs) => {
                let mut map = s.serialize_map(Some(vs.len()))?;
                for (k, v) in vs {
                    map.serialize_entry(&Bytes::new(k), v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserializer<'de> for &'de Value {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self {
            Value::Int(i) => visitor.visit_i64(*i),
            Value::Bytes(v) => visitor.visit_bytes(v.as_slice()),
            Value::List(v) => {
                let mut seq = de::value::SeqDeserializer::new(v.iter());
                visitor.visit_seq(&mut seq)
            }
            Value::Dict(v) => {
                let mut map =
                    de::value::MapDeserializer::new(v.iter().map(|(k, v)| (k.as_slice(), v)));
                visitor.visit_map(&mut map)
            }
        }
    }

    // bencoded strings are just bytes, so we attempt to treat them as UTF-8 here
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self {
            Value::Bytes(v) => {
                let s = String::from_utf8(v.clone()).map_err(|e| {
                    Error::invalid_value(Unexpected::Bytes(&e.into_bytes()), &"valid utf-8")
                })?;
                visitor.visit_string(s)
            }
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self {
            Value::Bytes(v) => {
                let s = String::from_utf8(v.clone()).map_err(|e| {
                    Error::invalid_value(Unexpected::Bytes(&e.into_bytes()), &"valid utf-8")
                })?;
                visitor.visit_enum(s.into_deserializer())
            }
            Value::Dict(ref v) => {
                let (key, value) = v
                    .iter()
                    .next()
                    .ok_or(Error::invalid_length(v.len(), &"map with 1 entry"))?;
                let s = str::from_utf8(key)
                    .map_err(|_| Error::invalid_value(Unexpected::Bytes(key), &"valid utf-8"))?;
                visitor.visit_enum(EnumDeserializer {
                    variant: s,
                    value: &value,
                })
            }
            _ => Err(Error::invalid_type(Unexpected::Unit, &"string or map")),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    fn deserialize_bool<V>(self, visitor: V) -> result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        let val = match self {
            Value::Int(i) => Ok(*i == 1),
            Value::Bytes(e) => Err(Error::invalid_type(Unexpected::Bytes(e), &"bool")),
            Value::List(_) => Err(Error::invalid_type(Unexpected::Seq, &"bool")),
            Value::Dict(_) => Err(Error::invalid_type(Unexpected::Map, &"bool")),
        }?;
        visitor.visit_bool(val)
    }

    forward_to_deserialize_any! {
        char i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 unit unit_struct
        seq map tuple_struct struct identifier tuple ignored_any bytes
        byte_buf newtype_struct
    }
}

impl<'de> IntoDeserializer<'de, Error> for &'de Value {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

#[derive(Debug)]
struct ValueVisitor;
impl<'de> de::Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any valid bencode value")
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> result::Result<Value, E> {
        Ok(Value::Int(value))
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> result::Result<Value, E> {
        Ok(Value::Int(value as i64))
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> result::Result<Value, E>
    where
        E: de::Error,
    {
        Ok(Value::Bytes(value.into()))
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> result::Result<Value, E> {
        Ok(Value::Bytes(value.into()))
    }

    #[inline]
    fn visit_bytes<E>(self, value: &[u8]) -> result::Result<Value, E> {
        Ok(Value::Bytes(value.into()))
    }

    #[inline]
    fn visit_seq<V>(self, mut access: V) -> result::Result<Value, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let mut seq = Vec::new();
        while let Some(e) = access.next_element()? {
            seq.push(e);
        }
        Ok(Value::List(seq))
    }

    #[inline]
    fn visit_map<V>(self, mut access: V) -> result::Result<Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let mut map = HashMap::new();
        while let Some((k, v)) = access.next_entry::<ByteBuf, _>()? {
            map.insert(k.into_vec(), v);
        }
        Ok(Value::Dict(map))
    }
}

impl<'de> de::Deserialize<'de> for Value {
    #[inline]
    fn deserialize<D>(deserializer: D) -> result::Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::Int(v)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Bytes(s.into_bytes())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(v: &str) -> Value {
        Value::Bytes(v.as_bytes().to_vec())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Value {
        Value::Bytes(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Value {
        Value::List(v)
    }
}

impl From<HashMap<Vec<u8>, Value>> for Value {
    fn from(v: HashMap<Vec<u8>, Value>) -> Value {
        Value::Dict(v)
    }
}

#[derive(Debug)]
struct EnumDeserializer<'de> {
    variant: &'de str,
    value: &'de Value,
}

impl<'de> de::EnumAccess<'de> for EnumDeserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let variant_de: de::value::StrDeserializer<Error> = self.variant.into_deserializer();
        let variant = seed.deserialize(variant_de)?;
        Ok((variant, self))
    }
}

impl<'de> de::VariantAccess<'de> for EnumDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        de::Deserialize::deserialize(self.value)
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self.value)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        Deserializer::deserialize_seq(self.value, visitor)
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        Deserializer::deserialize_map(self.value, visitor)
    }
}

/// Deserialize a [Value] instance into concrete type `T`.
pub fn from_value<'de, T>(value: &'de Value) -> Result<T>
where
    T: de::Deserialize<'de>,
{
    T::deserialize(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        pub id: u64,
        pub enabled: bool,
        #[serde(default)]
        pub version: Option<String>,
    }

    mod from_value {
        use super::*;

        #[test]
        fn test_deserialize() {
            let expected_result = TestStruct {
                id: 1,
                enabled: true,
                version: Some("my-version".to_string()),
            };

            let bytes = bencode::to_bytes(&expected_result).unwrap();
            let value: Value = bencode::from_bytes(bytes.as_slice()).unwrap();

            let result: TestStruct = from_value(&value).expect("expected value to be deserialized");
            assert_eq!(
                expected_result, result,
                "expected deserialized value to match expected"
            )
        }

        #[test]
        fn test_deserialize_none_field() {
            let expected_result = TestStruct {
                id: 67,
                enabled: false,
                version: None,
            };

            let message = bencode::to_string(&expected_result).unwrap();
            let value: Value = bencode::from_str(message.as_str()).unwrap();

            let result: TestStruct = from_value(&value).expect("expected value to be deserialized");
            assert_eq!(
                expected_result, result,
                "expected deserialized value to match expected"
            )
        }
    }
}
