use crate::metrics::Metric;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::{PeerDiscovery, PeerEvent};
use crate::{TorrentContext, TorrentEvent};
use async_trait::async_trait;
use fx_callback::{Callback, Subscription};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The torrent stats operation collects metrics from the torrent peers and publishes them via the [TorrentEvent::Stats] event.
#[derive(Debug)]
pub struct TorrentStatsOperation {
    last_tick: Instant,
    initialized: bool,
    event_receiver: Option<Subscription<TorrentEvent>>,
    peer_receivers: Vec<Subscription<PeerEvent>>,
}

impl TorrentStatsOperation {
    pub fn new() -> Self {
        Self {
            last_tick: Instant::now(),
            initialized: false,
            event_receiver: None,
            peer_receivers: vec![],
        }
    }

    fn interval(&self) -> Duration {
        self.last_tick.elapsed().max(Duration::from_millis(1))
    }

    fn initialize(&mut self, context: &TorrentContext) {
        if self.initialized {
            return;
        }

        self.event_receiver = Some(context.subscribe());
        self.initialized = true;
    }

    fn process_torrent_events(&mut self, context: &TorrentContext) {
        let receiver = match &mut self.event_receiver {
            Some(receiver) => receiver,
            None => return,
        };

        while let Ok(event) = receiver.try_recv() {
            if let TorrentEvent::PeerConnected(peer) = &*event {
                let peer = match context.peer_pool().get(&peer.handle) {
                    Some(peer) => peer,
                    None => continue,
                };

                self.peer_receivers.push(peer.subscribe());
            }
        }
    }

    fn process_peer_events(&mut self, context: &TorrentContext) {
        let metrics = context.metrics();

        for receiver in &mut self.peer_receivers {
            while let Ok(event) = receiver.try_recv() {
                if let PeerEvent::Stats(stats) = &*event {
                    metrics.upload.inc_by(stats.bytes_out.get());
                    metrics.upload_useful.inc_by(stats.bytes_out_useful.get());
                    metrics.download.inc_by(stats.bytes_in.get());
                    metrics.download_useful.inc_by(stats.bytes_in_useful.get());
                }
            }
        }

        self.peer_receivers.retain(|e| !e.is_closed());
    }
}

#[async_trait]
impl TorrentOperation for TorrentStatsOperation {
    fn name(&self) -> &str {
        "torrent stats operation"
    }

    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[Arc<dyn PeerDiscovery>],
    ) -> TorrentOperationResult {
        self.initialize(context);
        self.process_torrent_events(context);

        // collect the peer metrics
        self.process_peer_events(context);

        // invoke the stats event for the torrent
        let stats = context.metrics().snapshot();
        let interval = self.interval();
        context.metrics().tick(interval);
        context.invoke_event(TorrentEvent::Stats(stats));
        self.last_tick = Instant::now();

        TorrentOperationResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::Peer;
    use crate::{create_peer_pair, create_torrent, create_torrent_context, TorrentEvent};
    use fx_callback::Callback;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_execute() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (tx, rx) = oneshot::channel();
        let torrent = create_torrent!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let (source, _target) = create_peer_pair!(&torrent);
        let mut operation = TorrentStatsOperation::new();

        // initialize the operation
        // this makes the operation subscribe to the torrent context events
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);
        assert_eq!(
            true, operation.initialized,
            "expected the operation to be initialized"
        );

        // add the peer to the peer pool
        let source_client = source.client();
        let result = context.peer_pool_mut().add_peer(Box::new(source));
        assert!(
            result.is_ok(),
            "expected the peer to be added, but got {:?}",
            result
        );

        // invoked the PeerConnected event for the torrent context
        // this makes the operation subscribe to the peer events
        context.invoke_event(TorrentEvent::PeerConnected(source_client));

        // subscribe to the torrent context events
        let mut receiver = context.subscribe();
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if let TorrentEvent::Stats(metrics) = &*event {
                    let _ = tx.send(metrics.clone());
                    break;
                }
            }
        });

        // execute the operation
        let result = operation.execute(&mut context, vec![].as_slice()).await;
        assert_eq!(TorrentOperationResult::Continue, result);

        let _ = timeout(Duration::from_millis(100), rx)
            .await
            .expect("expected to have received a TorrentEvent::Stats event")
            .unwrap();
    }
}
