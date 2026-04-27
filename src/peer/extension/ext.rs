#[cfg(feature = "extension-donthave")]
use crate::peer::extension::DontHaveExtension;
#[cfg(feature = "extension-holepunch")]
use crate::peer::extension::HolepunchExtension;
#[cfg(feature = "extension-metadata")]
use crate::peer::extension::MetadataExtension;
#[cfg(feature = "extension-pex")]
use crate::peer::extension::PexExtension;
use crate::peer::PeerContext;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;

/// The extension unique name
pub type ExtensionName = String;
/// The extension unique identifier
pub type ExtensionNumber = u8;
/// The registry of the known extensions and their identifiers
pub type ExtensionRegistry = HashMap<ExtensionName, ExtensionNumber>;

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

    /// Process an incoming extension message payload for the extension which has been received from the remote peer.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload message of the extension from the remote peer.
    /// * `peer` - The peer context that received the payload message.
    ///
    /// # Returns
    ///
    /// Return an error when the extension fails to process the payload successfully.
    async fn on_message(
        &mut self,
        payload: &[u8],
        peer: &mut PeerContext,
    ) -> crate::peer::extension::Result<()>;

    /// Invoked once per tick (typically once per second), providing a tick interval for the extension
    /// to process data.
    async fn tick(&mut self, peer: &mut PeerContext);
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

    /// Process an incoming extension message payload which has been received from the remote peer.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn on_message(
        &mut self,
        payload: &[u8],
        peer: &mut PeerContext,
    ) -> crate::peer::extension::Result<()> {
        match self {
            #[cfg(feature = "extension-donthave")]
            PeerExtension::DontHave(e) => e.on_message(payload, peer).await,
            #[cfg(feature = "extension-holepunch")]
            PeerExtension::Holepunch(e) => e.on_message(payload, peer).await,
            #[cfg(feature = "extension-metadata")]
            PeerExtension::Metadata(e) => e.on_message(payload, peer).await,
            #[cfg(feature = "extension-pex")]
            PeerExtension::Pex(e) => e.on_message(payload, peer).await,
            PeerExtension::Other(e) => e.on_message(payload, peer).await,
        }
    }

    /// Invoked once per tick (typically once per second), providing a tick interval for the extension
    /// to process data.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn tick(&mut self, peer: &mut PeerContext) {
        match self {
            #[cfg(feature = "extension-metadata")]
            PeerExtension::Metadata(e) => e.tick(peer).await,
            #[cfg(feature = "extension-pex")]
            PeerExtension::Pex(e) => e.tick(peer).await,
            PeerExtension::Other(e) => e.tick(peer).await,
            _ => {}
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

impl<E> From<E> for PeerExtension
where
    E: Extension + 'static,
{
    fn from(extension: E) -> Self {
        Self::Other(Box::new(extension))
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

        async fn on_message(
            &mut self,
            _: &[u8],
            _: &mut PeerContext,
        ) -> crate::peer::extension::Result<()> {
            Ok(())
        }

        async fn tick(&mut self, _: &mut PeerContext) {
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
            let extension: PeerExtension = TestCustomExtension.into();
            assert_eq!(extension.name(), "test_custom_extension");
        }
    }
}
