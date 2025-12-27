#[cfg(feature = "dht")]
use crate::dht::DhtTracker;

/// A wrapper for the DHT tracker option.
#[derive(Debug, Clone)]
pub struct DhtOption {
    #[cfg(feature = "dht")]
    pub inner: Option<DhtTracker>,
    #[cfg(not(feature = "dht"))]
    _private: (),
}

#[cfg(feature = "dht")]
impl DhtOption {
    /// Create a new DHT option for the given tracker.
    pub fn new(dht: DhtTracker) -> Self {
        Self { inner: Some(dht) }
    }

    /// Create a new DHT option with no tracker.
    pub fn none() -> Self {
        Self { inner: None }
    }

    /// Set the DHT tracker for the option.
    pub fn set(&mut self, dht: DhtTracker) {
        self.inner = Some(dht);
    }
}

#[cfg(not(feature = "dht"))]
impl DhtOption {
    /// Create a new DHT option with no tracker.
    pub fn none() -> Self {
        Self { _private: () }
    }
}

impl Default for DhtOption {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(feature = "dht")]
impl From<Option<DhtTracker>> for DhtOption {
    fn from(dht: Option<DhtTracker>) -> Self {
        Self { inner: dht }
    }
}
