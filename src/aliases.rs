use fx_handle::Handle;

/// A unique handle identifier of a [crate::FxSession].
pub type SessionHandle = Handle;

/// A unique handle identifier of a [crate::Torrent].
pub type TorrentHandle = Handle;

/// The bitfield vector type used by the library.
pub type BitVec = bitvec::vec::BitVec<u8, bitvec::order::Msb0>;

/// The bitfield slice type used by the library.
pub type BitSlice = bitvec::slice::BitSlice<u8, bitvec::order::Msb0>;
