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

    /// Returns `true` when no DHT tracker is set.
    pub fn is_none(&self) -> bool {
        self.inner.is_none()
    }
}

#[cfg(not(feature = "dht"))]
impl DhtOption {
    /// Create a new DHT option with no tracker.
    pub fn none() -> Self {
        Self { _private: () }
    }

    /// Always returns `true` as the `dht` feature has been disabled.
    pub fn is_none(&self) -> bool {
        true
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_none() {
        let dht = DhtOption::none();

        let result = dht.is_none();

        assert_eq!(true, result);
    }

    #[tokio::test]
    async fn test_from_tracker() {
        let tracker = DhtTracker::builder().build().await.unwrap();
        let dht = DhtOption::from(Some(tracker));

        let result = dht.is_none();

        assert_eq!(false, result, "expected the DHT option to be present");
    }
}
