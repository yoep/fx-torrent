use crate::metrics::Metric;
use crate::operation::{TorrentOperation, TorrentOperationResult};
use crate::peer::PeerDiscovery;
use crate::{TorrentContext, TorrentEvent};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "tracing")]
use tracing::instrument;

/// The torrent stats operation collects metrics from the torrent peers and publishes them via the [TorrentEvent::Stats] event.
#[derive(Debug)]
pub struct TorrentStatsOperation {
    last_tick: Instant,
}

impl TorrentStatsOperation {
    pub fn new() -> Self {
        Self {
            last_tick: Instant::now(),
        }
    }

    fn interval(&self) -> Duration {
        self.last_tick.elapsed().max(Duration::from_millis(1))
    }

    fn collect_peer_metrics(&self, context: &TorrentContext) {
        let metrics = context.metrics();

        for (_, peer) in context.peer_pool().peers.iter() {
            let peer_metrics = peer.metrics();
            metrics.upload.inc_by(peer_metrics.bytes_out.get());
            metrics
                .upload_useful
                .inc_by(peer_metrics.bytes_out_useful.get());
            metrics.download.inc_by(peer_metrics.bytes_in.get());
            metrics
                .download_useful
                .inc_by(peer_metrics.bytes_in_useful.get());
            peer_metrics.tick(self.interval());
        }
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
        // collect the peer metrics
        self.collect_peer_metrics(context);

        // invoke the stats event for the torrent
        context.invoke_event(TorrentEvent::Stats(context.metrics().snapshot()));
        context.metrics().tick(self.interval());

        TorrentOperationResult::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{create_torrent_context, init_logger, TorrentEvent};
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
        let (mut context, _) = create_torrent_context!(
            "debian-udp.torrent",
            temp_path,
            TorrentFlags::none(),
            TorrentConfig::builder().build(),
            vec![]
        );
        let mut operation = TorrentStatsOperation::new();

        // subscribe to the torrent event
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
