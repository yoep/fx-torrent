use crate::metrics::Metric;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::{PeerDiscovery, PeerEvent};
use crate::{TorrentContext, TorrentEvent};
use async_trait::async_trait;
use fx_callback::{Callback, Subscription};
use std::time::{Duration, Instant};

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

    /// Returns `true` if the event invocation is allowed, else `false`.
    fn is_invocation_allowed(&self, context: &TorrentContext) -> bool {
        self.initialized && !context.is_paused()
    }

    /// Returns the current tick interval.
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

    fn tick(&mut self, context: &TorrentContext) {
        let interval = self.interval();
        context.metrics().tick(interval);
        self.last_tick = Instant::now();

        self.invoke_event(context);
    }

    /// Invokes the [TorrentEvent::Stats] event if allowed.
    fn invoke_event(&self, context: &TorrentContext) {
        if !self.is_invocation_allowed(context) {
            return;
        }

        let stats = context.metrics().snapshot();
        context.invoke_event(TorrentEvent::Stats(stats));
    }
}

#[async_trait]
impl TorrentOperation for TorrentStatsOperation {
    fn name(&self) -> &str {
        "torrent stats operation"
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn execute(
        &mut self,
        context: &mut TorrentContext,
        _: &[PeerDiscovery],
    ) -> TorrentOperationResult {
        self.initialize(context);

        // process all pending events
        self.process_torrent_events(context);
        self.process_peer_events(context);

        self.tick(context);
        TorrentOperationResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TorrentEvent, TorrentFlags};
    use fx_callback::Callback;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_is_invocation_allowed() {
        init_logger!();
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path().to_str().unwrap();
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::Paused,
            TorrentConfig::builder().build(),
            vec![],
            None
        );
        let mut operation = TorrentStatsOperation::new();

        // initialize the operation
        operation.initialize(&context);

        // should return false when the torrent is paused
        let result = operation.is_invocation_allowed(&context);
        assert_eq!(false, result);

        // should return true when the torrent is not paused
        context.remove_options(TorrentFlags::Paused);
        let result = operation.is_invocation_allowed(&context);
        assert_eq!(true, result);
    }

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
            vec![],
            None
        );
        let (source, _target) = create_tcp_peer_pair!(&torrent);
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
        let source_client = source.client_info().clone();
        let result = context.peer_pool_mut().add_peer(source.into());
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
            while let Ok(event) = receiver.recv().await {
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
