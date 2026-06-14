pub use bt::*;
pub use close_reason::*;
pub use utp_ext::*;
use utp_message::*;
use utp_packet::*;
pub use utp_socket::*;
pub use utp_stream::*;

mod bt;
pub(crate) mod close_reason;
mod utp_ext;
mod utp_message;
mod utp_packet;
mod utp_socket;
mod utp_stream;

/// The maximum size of a single uTP packet (= max UDP size).
const MAX_PACKET_SIZE: usize = 65_535;
/// The maximum size of a payload in a single uTP packet (= max UDP size - max uTP header size).
const MAX_PACKET_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - 26;
