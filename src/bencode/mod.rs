/*!
Serialize and deserialize bencoded data through [Serde](https://github.com/serde-rs/serde).
This module is based on the original [serde_bencode](https://crates.io/crates/serde_bencode) crate.
*/

pub use de::*;
pub use error::*;
pub use serialize::*;
pub use value::*;

mod de;
mod error;
mod ser;
mod serialize;
mod value;
