#[cfg(feature = "extension-donthave")]
use crate::peer::extension::DontHaveExtension;
#[cfg(feature = "extension-holepunch")]
use crate::peer::extension::HolepunchExtension;
#[cfg(feature = "extension-metadata")]
use crate::peer::extension::MetadataExtension;
#[cfg(feature = "extension-pex")]
use crate::peer::extension::PexExtension;
use crate::peer::{PeerContext, PeerEvent};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;

/// The extension unique name
pub type ExtensionName = String;
/// The extension unique identifier
pub type ExtensionNumber = u8;
/// The registry of the known extensions and their identifiers
pub type ExtensionRegistry = HashMap<ExtensionName, ExtensionNumber>;
/// The list type of enabled extensions
pub type Extensions = Vec<PeerExtension>;

/// A peer extension that is used within the BitTorrent protocol.
/// An extension can only be activated when the remote peer supports **BEP10**.
///
/// Extensions are registered at the [crate::Session] level.
/// An extension is then cloned through the [Extension::clone_boxed] method for each created peer connection in a torrent.
/// This means that the extension can store peer related information internally for later use.
#[async_trait]
pub trait Extension: Debug + Send + Sync {
    /// Returns the unique name of the extension.
    fn name(&self) -> &str;

    /// Handle the given extension message payload which has been received from the remote peer.
    /// If you want to store data internally, make use of [tokio::sync::Mutex] or [tokio::sync::RwLock].
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload message of the extension from the remote peer
    /// * `command_sender` - The command sender to interact with the underlying peer
    ///
    /// # Returns
    ///
    /// Return an error when the extension fails to process the payload successfully.
    async fn handle<'a>(
        &'a self,
        payload: &'a [u8],
        peer: &'a PeerContext,
    ) -> crate::peer::extension::Result<()>;

    /// Invoked when an event is raised by a peer and this extension is supported.
    /// Keep in mind that the [PeerEvent::HandshakeCompleted] event will never be received by an extension
    /// as the supported remote extensions are only known after the extended handshake.
    ///
    /// # Arguments
    ///
    /// * `event` - The event raised by the peer
    /// * `peer` - The peer that raised the event
    async fn on<'a>(&'a self, event: &'a PeerEvent, peer: &'a PeerContext);
}

/// A peer extension that is used within the BitTorrent protocol.
#[derive(Debug)]
pub enum PeerExtension {
    #[cfg(feature = "extension-donthave")]
    DontHave(DontHaveExtension),
    #[cfg(feature = "extension-holepunch")]
    Holepunch(HolepunchExtension),
    #[cfg(feature = "extension-metadata")]
    Metadata(MetadataExtension),
    #[cfg(feature = "extension-pex")]
    Pex(PexExtension),
    Other(Box<dyn Extension>),
}

impl PeerExtension {
    /// Returns the unique name of the extension.
    pub fn name(&self) -> &str {
        match self {
            #[cfg(feature = "extension-donthave")]
            PeerExtension::DontHave(_) => DontHaveExtension::NAME,
            #[cfg(feature = "extension-holepunch")]
            PeerExtension::Holepunch(_) => HolepunchExtension::NAME,
            #[cfg(feature = "extension-metadata")]
            PeerExtension::Metadata(_) => MetadataExtension::NAME,
            #[cfg(feature = "extension-pex")]
            PeerExtension::Pex(_) => PexExtension::NAME,
            PeerExtension::Other(e) => e.name(),
        }
    }

    /// Handle the given extension message payload which has been received from the remote peer.
    pub async fn handle<'a>(
        &'a self,
        payload: &'a [u8],
        peer: &'a PeerContext,
    ) -> crate::peer::extension::Result<()> {
        match self {
            #[cfg(feature = "extension-donthave")]
            PeerExtension::DontHave(e) => e.handle(payload, peer).await,
            #[cfg(feature = "extension-holepunch")]
            PeerExtension::Holepunch(e) => e.handle(payload, peer).await,
            #[cfg(feature = "extension-metadata")]
            PeerExtension::Metadata(e) => e.handle(payload, peer).await,
            #[cfg(feature = "extension-pex")]
            PeerExtension::Pex(e) => e.handle(payload, peer).await,
            PeerExtension::Other(e) => e.handle(payload, peer).await,
        }
    }

    /// Process a peer event for the extension.
    pub async fn on<'a>(&'a self, event: &'a PeerEvent, peer: &'a PeerContext) {
        match self {
            #[cfg(feature = "extension-donthave")]
            PeerExtension::DontHave(_) => {} // no-op
            #[cfg(feature = "extension-holepunch")]
            PeerExtension::Holepunch(_) => {} // no-op
            #[cfg(feature = "extension-metadata")]
            PeerExtension::Metadata(e) => e.on(event, peer).await,
            #[cfg(feature = "extension-pex")]
            PeerExtension::Pex(e) => e.on(event, peer).await,
            PeerExtension::Other(e) => e.on(event, peer).await,
        }
    }
}

#[cfg(feature = "extension-donthave")]
impl From<DontHaveExtension> for PeerExtension {
    fn from(extension: DontHaveExtension) -> Self {
        Self::DontHave(extension)
    }
}

#[cfg(feature = "extension-holepunch")]
impl From<HolepunchExtension> for PeerExtension {
    fn from(extension: HolepunchExtension) -> Self {
        Self::Holepunch(extension)
    }
}

#[cfg(feature = "extension-metadata")]
impl From<MetadataExtension> for PeerExtension {
    fn from(extension: MetadataExtension) -> Self {
        Self::Metadata(extension)
    }
}

#[cfg(feature = "extension-pex")]
impl From<PexExtension> for PeerExtension {
    fn from(extension: PexExtension) -> Self {
        Self::Pex(extension)
    }
}

impl From<Box<dyn Extension>> for PeerExtension {
    fn from(extension: Box<dyn Extension>) -> Self {
        Self::Other(extension)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestCustomExtension;

    #[async_trait]
    impl Extension for TestCustomExtension {
        fn name(&self) -> &str {
            "test_custom_extension"
        }

        async fn handle<'a>(
            &'a self,
            _: &'a [u8],
            _: &'a PeerContext,
        ) -> crate::peer::extension::Result<()> {
            Ok(())
        }

        async fn on<'a>(&'a self, _: &'a PeerEvent, _: &'a PeerContext) {
            // no-op
        }
    }

    mod name {
        use super::*;

        #[test]
        fn test_standard_extensions() {
            let extension: PeerExtension = DontHaveExtension::new().into();
            assert_eq!(extension.name(), DontHaveExtension::NAME);

            let extension: PeerExtension = HolepunchExtension::new().into();
            assert_eq!(extension.name(), HolepunchExtension::NAME);

            let extension: PeerExtension = MetadataExtension::new().into();
            assert_eq!(extension.name(), MetadataExtension::NAME);

            let extension: PeerExtension = PexExtension::new().into();
            assert_eq!(extension.name(), PexExtension::NAME);
        }

        #[test]
        fn test_custom_extensions() {
            let extension: PeerExtension =
                (Box::new(TestCustomExtension) as Box<dyn Extension>).into();
            assert_eq!(extension.name(), "test_custom_extension");
        }
    }
}
