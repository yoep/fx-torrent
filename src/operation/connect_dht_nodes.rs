use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::TorrentContext;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;
use log::{debug, trace};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::net::lookup_host;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// Connect to the DHT nodes defined within the torrent metadata.
pub struct TorrentDhtNodesOperation {
    initialized: bool,
    in_flight: Option<BoxFuture<'static, ()>>,
}

impl TorrentDhtNodesOperation {
    pub fn new() -> Self {
        Self {
            initialized: false,
            in_flight: None,
        }
    }

    /// Poll the currently in-flight DHT operation for completion.
    fn poll_in_flight(&mut self) {
        if self
            .in_flight
            .as_mut()
            .map(|e| e.now_or_never())
            .flatten()
            .is_some()
        {
            self.initialized = true;
            self.in_flight = None;
        }
    }

    /// Check if the operation should connect to the DHT nodes of the torrent.
    fn should_connect_to_dht_nodes(&self, context: &TorrentContext) -> bool {
        if context.dht().is_none() {
            return false;
        }

        !self.initialized && self.in_flight.is_none()
    }

    async fn connect_dht_nodes(&mut self, context: &TorrentContext) {
        let handle = context.handle();
        let metadata = context.metadata();
        let dht = match context.dht().inner.as_ref() {
            None => return,
            Some(dht) => dht.clone(),
        };
        let nodes = match metadata.nodes.clone() {
            None => {
                self.initialized = true;
                return;
            }
            Some(nodes) => nodes,
        };

        trace!(
            "Torrent {} is trying to add {} DHT node(s)",
            context,
            nodes.len()
        );
        self.in_flight = Some(Box::pin(async move {
            let mut futures: Vec<_> = vec![];
            for node in nodes.iter() {
                // try to parse the host of the node as an IP address
                // if it fails, we assume it's a DNS name and try to resolve it
                match node.socket_addr() {
                    Ok(addr) => futures.push(dht.ping(addr)),
                    Err(_) => {
                        if let Ok(addrs) =
                            lookup_host(format!("{}:{}", node.host.as_str(), node.port)).await
                        {
                            for addr in addrs {
                                futures.push(dht.ping(addr));
                            }
                        }
                    }
                }
            }

            let pinged_nodes = futures::future::join_all(futures)
                .await
                .into_iter()
                .filter(|e| e.is_ok())
                .count();
            debug!(
                "Torrent {} pinged a total of {} DHT nodes",
                handle, pinged_nodes
            );
        }));
    }
}

#[async_trait]
impl TorrentOperation for TorrentDhtNodesOperation {
    fn name(&self) -> &str {
        "connect torrent DHT nodes operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        self.poll_in_flight();

        if self.should_connect_to_dht_nodes(context) {
            self.connect_dht_nodes(context).await;
        }

        TorrentOperationResult::Continue
    }
}

impl Debug for TorrentDhtNodesOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentDhtNodesOperation")
            .field("initialized", &self.initialized)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent_context;
    use crate::dht::DhtTracker;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&nodes=127.0.0.1&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let dht = DhtTracker::builder()
            .default_routing_nodes()
            .build()
            .await
            .unwrap();
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![],
            Some(dht)
        );
        let mut operation = TorrentDhtNodesOperation::new();

        // poll the initial operation
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(
            TorrentOperationResult::Continue,
            result,
            "expected the DHT nodes to have been initialized"
        );
        assert_eq!(
            true, operation.initialized,
            "expected the operation to not have been completed"
        );

        // keep polling till the operation is completed
        timeout!(
            async {
                while !operation.initialized {
                    let _ = operation.execute(&mut context, vec![].as_slice()).await;
                }
            },
            Duration::from_secs(5)
        );
        let result = operation.initialized;
        assert_eq!(true, result, "expected the operation to be completed");
    }

    mod should_connect_to_dht_nodes {
        use super::*;

        #[tokio::test]
        async fn test_dht_none() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
            let (context, _) = create_torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                None
            );
            let operation = TorrentDhtNodesOperation::new();

            let result = operation.should_connect_to_dht_nodes(&context);

            assert_eq!(
                false, result,
                "expected the operation to not connect to DHT nodes"
            );
        }

        #[tokio::test]
        async fn test_initialized() {
            init_logger!();
            let temp_dir = tempdir().unwrap();
            let temp_path = temp_dir.path().to_str().unwrap();
            let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
            let dht = DhtTracker::builder()
                .default_routing_nodes()
                .build()
                .await
                .unwrap();
            let (context, _) = create_torrent_context!(
                uri,
                temp_path,
                TorrentFlags::none(),
                TorrentConfig::builder().build(),
                vec![],
                Some(dht)
            );
            let mut operation = TorrentDhtNodesOperation::new();

            // if we're not yet initialized, we should connect to DHT nodes
            let result = operation.should_connect_to_dht_nodes(&context);
            assert_eq!(
                true, result,
                "expected the operation to connect to DHT nodes"
            );

            // if we're not yet initialized, but we have an in-flight operation
            // we should not connect to DHT nodes
            operation.in_flight = Some(Box::pin(async move {}));
            let result = operation.should_connect_to_dht_nodes(&context);
            assert_eq!(
                false, result,
                "expected the operation to not connect to DHT nodes"
            );

            // if we're fully initialized, we should not connect to DHT nodes
            operation.in_flight = None;
            operation.initialized = true;
            let result = operation.should_connect_to_dht_nodes(&context);
            assert_eq!(
                false, result,
                "expected the operation to not connect to DHT nodes"
            );
        }
    }
}
