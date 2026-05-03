use crate::bencode::ser::string;
use crate::bencode::{Error, Serializer};
use serde::ser;

/// The Map serializer of the bencode format.
pub struct SerializeMap<'a> {
    ser: &'a mut Serializer,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    cur_key: Option<Vec<u8>>,
}

impl<'a> SerializeMap<'a> {
    pub fn new(ser: &'a mut Serializer, len: usize) -> SerializeMap<'a> {
        SerializeMap {
            ser,
            entries: Vec::with_capacity(len),
            cur_key: None,
        }
    }

    fn end_map(&mut self) -> crate::bencode::Result<()> {
        if self.cur_key.is_some() {
            return Err(Error::InvalidValue(
                "`serialize_key` called without calling  `serialize_value`".to_string(),
            ));
        }
        let mut entries = std::mem::take(&mut self.entries);
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        self.ser.push("d");
        for (k, v) in entries {
            ser::Serializer::serialize_bytes(&mut *self.ser, k.as_ref())?;
            self.ser.push(v);
        }
        self.ser.push("e");
        Ok(())
    }
}

impl<'a> ser::SerializeMap for SerializeMap<'a> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: ?Sized + ser::Serialize>(&mut self, key: &T) -> crate::bencode::Result<()> {
        if self.cur_key.is_some() {
            return Err(Error::InvalidValue(
                "`serialize_key` called multiple times without calling  `serialize_value`"
                    .to_string(),
            ));
        }
        self.cur_key = Some(key.serialize(&mut string::Serializer)?);
        Ok(())
    }
    fn serialize_value<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> crate::bencode::Result<()> {
        let key = self.cur_key.take().ok_or_else(|| {
            Error::InvalidValue(
                "`serialize_value` called without calling `serialize_key`".to_string(),
            )
        })?;
        let mut ser = Serializer::new();
        value.serialize(&mut ser)?;
        let value = ser.into_vec();
        if !value.is_empty() {
            self.entries.push((key, value));
        }
        Ok(())
    }
    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> crate::bencode::Result<()>
    where
        K: ?Sized + ser::Serialize,
        V: ?Sized + ser::Serialize,
    {
        if self.cur_key.is_some() {
            return Err(Error::InvalidValue(
                "`serialize_key` called multiple times without calling  `serialize_value`"
                    .to_string(),
            ));
        }
        let key = key.serialize(&mut string::Serializer)?;
        let mut ser = Serializer::new();
        value.serialize(&mut ser)?;
        let value = ser.into_vec();
        if !value.is_empty() {
            self.entries.push((key, value));
        }
        Ok(())
    }
    fn end(mut self) -> crate::bencode::Result<()> {
        self.end_map()
    }
}

impl<'a> ser::SerializeStruct for SerializeMap<'a> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> crate::bencode::Result<()> {
        ser::SerializeMap::serialize_entry(self, key, value)
    }
    fn end(mut self) -> crate::bencode::Result<()> {
        self.end_map()
    }
}

impl<'a> ser::SerializeStructVariant for SerializeMap<'a> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> crate::bencode::Result<()> {
        ser::SerializeMap::serialize_entry(self, key, value)
    }
    fn end(mut self) -> crate::bencode::Result<()> {
        self.end_map()?;
        self.ser.push("e");
        Ok(())
    }
}
