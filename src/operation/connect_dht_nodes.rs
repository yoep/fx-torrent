use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::TorrentContext;
use async_trait::async_trait;
use log::{debug, trace};
use std::sync::Arc;
use tokio::net::lookup_host;
#[cfg(feature = "tracing")]
use tracing::instrument;

/// Connect to the DHT nodes defined within the torrent metadata.
#[derive(Debug)]
pub struct TorrentDhtNodesOperation {
    initialized: bool,
}

impl TorrentDhtNodesOperation {
    pub fn new() -> Self {
        Self { initialized: false }
    }

    /// Check if the operation should connect to the DHT nodes of the torrent.
    fn should_connect_to_dht_nodes(&self) -> bool {
        !self.initialized
    }

    async fn connect_dht_nodes(&mut self, context: &TorrentContext) {
        let handle = context.handle();
        let metadata = context.metadata();
        let dht = match context.dht().inner.as_ref() {
            None => return,
            Some(dht) => dht.clone(),
        };

        if let Some(nodes) = metadata.nodes.clone() {
            trace!(
                "Torrent {} is trying to add {} DHT node(s)",
                context,
                nodes.len()
            );

            tokio::spawn(async move {
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
            });
        } else {
            debug!("Torrent {} does not have any DHT nodes", context);
        }

        self.initialized = true;
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
        if self.should_connect_to_dht_nodes() {
            self.connect_dht_nodes(context).await;
        }

        TorrentOperationResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_torrent_context;
    use crate::init_logger;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let uri = "magnet:?xt=urn:btih:EADAF0EFEA39406914414D359E0EA16416409BD7&dn=debian-12.4.0-amd64-DVD-1.iso&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce";
        let (mut context, _) = create_torrent_context!(
            uri,
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentDhtNodesOperation::new();

        let result = operation.execute(&mut context, vec![].as_slice()).await;

        assert_eq!(
            TorrentOperationResult::Continue,
            result,
            "expected the DHT nodes to have been initialized"
        );
        assert_eq!(
            true, operation.initialized,
            "expected the operation to have been initialized"
        );
    }
}
