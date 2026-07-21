use crate::channel::ChannelSender;
use crate::dht::{Error, Node, NodeId, TrackerCommand, TrackerContext};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use log::trace;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;

const FIND_NODE_TIMEOUT: Duration = Duration::from_secs(6);

/// The DHT traversal algorithm to discover nodes in the DHT network.
#[derive(Debug)]
pub(crate) struct TraversalAlgorithm {
    queried: HashSet<SocketAddr>,
    unqueried: Vec<PendingQuery>,
    sender: ChannelSender<TrackerCommand>,
    permits: Arc<Semaphore>,
    limit: usize,
}

impl TraversalAlgorithm {
    /// Create a new traversal algorithm instance.
    ///
    /// # Arguments
    ///
    /// * `bucket_size` - The bucket size of the underlying node routing table.
    /// * `routing_nodes` - The bootstrap nodes to start the traversal with.
    /// * `sender` - The command sender to execute tasks on the main loop.
    pub fn new(
        bucket_size: usize,
        routing_nodes: Vec<SocketAddr>,
        sender: ChannelSender<TrackerCommand>,
    ) -> Self {
        Self {
            queried: Default::default(),
            unqueried: routing_nodes
                .into_iter()
                .map(|addr| PendingQuery { id: None, addr })
                .collect(),
            sender,
            permits: Arc::new(Semaphore::new(bucket_size * 2)),
            limit: bucket_size * 160, // = bucket size * max routing table buckets
        }
    }

    /// Execute the traversal algorithm for the given target node ID.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub async fn run(&mut self, target_id: NodeId, context: &mut TrackerContext) {
        if self.queried.len() >= self.limit
            || self.permits.available_permits() == 0
            || self.unqueried.is_empty()
        {
            return;
        }

        self.send_pending_queries(target_id, context).await;
        self.sort_unqueried_by_distance(&target_id);
    }

    /// Add the given node details to the traversal for querying.
    /// The node will be ignored if it has been queried before.
    pub fn add_node(&mut self, id: Option<NodeId>, addr: SocketAddr) {
        if self.queried.contains(&addr) || self.unqueried.iter().any(|e| e.addr == addr) {
            trace!("DHT traversal ignoring node, {} is already known", addr);
            return;
        }

        self.unqueried.push(PendingQuery { id, addr });
    }

    /// Start the traversal algorithm from scratch.
    /// This will remove all queried nodes from the traversal and restart the algorithm.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    pub fn restart(&mut self) {
        self.unqueried.extend(
            self.queried
                .drain()
                .map(|addr| PendingQuery { id: None, addr }),
        );
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn send_pending_queries(&mut self, target_id: NodeId, context: &mut TrackerContext) {
        let mut futures = FuturesUnordered::new();
        while let Some(query) = self.unqueried.pop() {
            if self.queried.contains(&query.addr) {
                continue;
            }

            let permit = match self.permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    // no more permits available, put it back in the unqueried list
                    self.unqueried.push(query);
                    break;
                }
            };

            self.queried.insert(query.addr);
            let node = Node::new(NodeId::from_ip(&query.addr.ip()), query.addr);
            let response = context.find_node(target_id, &node).await;
            futures.push(async move {
                let _permit = permit; // drops the permit when the query is completed
                timeout(FIND_NODE_TIMEOUT, response)
                    .await
                    .map_err(|_| Error::Timeout)
                    .flatten()
            });
        }

        if futures.is_empty() {
            return;
        }

        let command_sender = self.sender.clone();
        tokio::spawn(async move {
            while let Some(response) = futures.next().await {
                match response {
                    Ok(nodes) => {
                        trace!(
                            "DHT traversal discovered nodes, {:?}",
                            nodes.iter().map(|e| e.addr).collect::<Vec<_>>()
                        );
                        for node in nodes {
                            let _ = command_sender
                                .fire_and_forget(TrackerCommand::AddTraversalNode((
                                    node.id, node.addr,
                                )))
                                .await;
                        }
                    }
                    Err(e) => {
                        trace!("DHT traversal failed to query node, {}", e);
                    }
                }
            }
        });
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    fn sort_unqueried_by_distance(&mut self, target_id: &NodeId) {
        self.unqueried = self
            .unqueried
            .iter()
            .sorted_by(|a, b| match (a.id.as_ref(), b.id.as_ref()) {
                (Some(a), Some(b)) => {
                    let dist_a = target_id.distance(a);
                    let dist_b = target_id.distance(b);
                    dist_b.cmp(&dist_a)
                }
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .cloned()
            .collect();
    }
}

#[derive(Debug, Clone)]
struct PendingQuery {
    id: Option<NodeId>,
    addr: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::observer::Observer;
    use crate::dht::DhtEvent;
    use fx_callback::Callback;
    use std::net::Ipv4Addr;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[test]
    fn test_new() {
        init_logger!();
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 9000).into();
        let (sender, _receiver) = channel!(2);
        let traversal = TraversalAlgorithm::new(8, vec![addr], sender);

        assert_eq!(
            0,
            traversal.queried.len(),
            "expected the queried addresses to be empty"
        );
        assert_eq!(traversal.unqueried.len(), 1);
    }

    #[test]
    fn test_restart() {
        let (sender, _receiver) = channel!(2);
        let mut traversal = TraversalAlgorithm::new(8, vec![], sender);

        // insert a queried node
        traversal.queried.insert((Ipv4Addr::LOCALHOST, 9877).into());
        assert_eq!(
            1,
            traversal.queried.len(),
            "expected the queried addresses to contain the inserted node"
        );

        // restart the traversal algorithm
        traversal.restart();
        assert_eq!(
            0,
            traversal.queried.len(),
            "expected the queried addresses to be empty after restart"
        );
        assert_eq!(
            1,
            traversal.unqueried.len(),
            "expected the queried nodes to have been requeued after restart"
        );
    }

    #[tokio::test]
    async fn test_run() {
        init_logger!();
        let mut source = create_tracker_context!();
        let mut target = create_tracker_context!();
        let source_id = source.routing_table.id;
        let target_addr: SocketAddr = (Ipv4Addr::LOCALHOST, target.socket_addr.port()).into();
        let (tx, rx) = oneshot::channel();
        let (sender, _receiver) = channel!(2);
        let mut traversal = TraversalAlgorithm::new(8, vec![target_addr.clone()], sender.clone());

        // subscribe to the source node events
        let mut subscription = source.callbacks.subscribe();
        tokio::spawn(async move {
            while let Ok(event) = subscription.recv().await {
                if let DhtEvent::NodeAdded(node) = &*event {
                    tx.send(node.clone()).unwrap();
                    break;
                }
            }
        });

        // start the target main loop in a separate task
        tokio::spawn(async move {
            let (sender, receiver) = channel!(1);
            let observer = Observer::new(sender.clone());
            let traversal = TraversalAlgorithm::new(8, vec![], sender);

            target
                .run(Duration::from_secs(60), observer, traversal, receiver)
                .await;
        });

        // run the traversal algorithm
        traversal.run(source_id, &mut source).await;

        // start the source main loop in a separate task
        tokio::spawn(async move {
            let (sender, receiver) = channel!(1);
            let observer = Observer::new(sender.clone());
            let traversal = TraversalAlgorithm::new(8, vec![], sender);
            source
                .run(Duration::from_secs(60), observer, traversal, receiver)
                .await
        });

        // wait for a node to be discovered
        let _ =
            timeout!(Duration::from_millis(500), rx).expect("expected a node to have been added");

        // try to add the node again for traversing
        traversal.add_node(None, target_addr);

        let result = traversal.unqueried.len();
        assert_eq!(
            0, result,
            "expected the traversed node address to have been ignored"
        );
    }
}
