use crate::channel::ChannelSender;
use crate::dht::{Node, NodeId, TrackerCommand, TrackerContext};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use itertools::Itertools;
use log::trace;
use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// The DHT traversal algorithm to discover nodes in the DHT network.
#[derive(Debug)]
pub(crate) struct TraversalAlgorithm {
    queried: HashSet<SocketAddr>,
    unqueried: VecDeque<PendingQuery>,
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
            permits: Arc::new(Semaphore::new(bucket_size)),
            limit: bucket_size * 160, // = bucket size * max routing table buckets
        }
    }

    /// Execute the traversal algorithm for the given target node ID.
    pub async fn run(&mut self, target_id: NodeId, context: &mut TrackerContext) {
        if self.permits.available_permits() == 0 {
            return;
        }
        if self.unqueried.is_empty() {
            return;
        }
        if self.queried.len() >= self.limit {
            return;
        }

        self.send_pending_queries(target_id, context).await;
        self.sort_unqueried_by_distance(&target_id);
    }

    /// Add the given node details to the traversal for querying.
    /// The node will be ignored if it has been queried before.
    pub fn add_node(&mut self, id: Option<NodeId>, addr: SocketAddr) {
        if self.queried.contains(&addr) {
            trace!("DHT traversal ignoring node, {} already queried", addr);
            return;
        }

        self.unqueried.push_back(PendingQuery { id, addr });
    }

    /// Start the traversal algorithm from scratch.
    /// This will remove all queried nodes from the traversal and restart the algorithm.
    pub fn restart(&mut self) {
        for addr in self.queried.drain().collect::<Vec<_>>() {
            self.add_node(None, addr);
        }
    }

    async fn send_pending_queries(&mut self, target_id: NodeId, context: &mut TrackerContext) {
        let mut queries = vec![];
        while let Some(query) = self.unqueried.pop_front() {
            if self.queried.contains(&query.addr) {
                continue;
            }

            let permit = match self.permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => break,
            };

            self.queried.insert(query.addr);
            let node = Node::new(NodeId::from_ip(&query.addr.ip()), query.addr);
            let response = context.find_node(target_id, &node).await;
            queries.push(async move { (permit, response.await) });
        }

        let command_sender = self.sender.clone();
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::from_iter(queries);
            while let Some((permit, response)) = futures.next().await {
                drop(permit);
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

    fn sort_unqueried_by_distance(&mut self, target_id: &NodeId) {
        self.unqueried = self
            .unqueried
            .iter()
            .sorted_by(|a, b| match (a.id.as_ref(), b.id.as_ref()) {
                (Some(a), Some(b)) => {
                    let a_distance = target_id.distance(a);
                    let b_distance = target_id.distance(b);
                    a_distance.cmp(&b_distance)
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
    use crate::dht::peers::PeerStorage;
    use crate::dht::{DhtEvent, Event};
    use crate::{channel, create_tracker_context, init_logger};
    use fx_callback::Callback;
    use std::net::Ipv4Addr;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::{select, time};

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
        let target_id = target.routing_table.id;
        let target_addr: SocketAddr = (Ipv4Addr::LOCALHOST, target.socket_addr.port()).into();
        let (tx, mut rx) = oneshot::channel();
        let (sender, _receiver) = channel!(2);
        let mut traversal = TraversalAlgorithm::new(8, vec![target_addr.clone()], sender.clone());

        // subscribe to the source node events
        let mut subscription = source.callbacks.subscribe();
        tokio::spawn(async move {
            while let Some(event) = subscription.recv().await {
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

            target.start(observer, traversal, receiver).await;
        });

        // run the traversal algorithm
        traversal.run(source_id, &mut source).await;

        // process the incoming message
        let mut observer = Observer::new(sender);
        let mut peers = PeerStorage::new();
        let timeout = time::sleep(Duration::from_millis(750));
        tokio::pin!(timeout);
        let result = loop {
            select! {
                _ = &mut timeout => {
                    assert!(false, "timeout waiting for node to be added");
                },
                result = &mut rx => {
                    break result;
                },
                Some(message) = source.receiver.recv() => {
                    source.run(Event::Incoming(message), &mut observer, &mut traversal, &mut peers).await;
                }
            }
        }
            .unwrap();
        assert_eq!(
            target_id, result.id,
            "expected the target node to have been added"
        );

        // try to add the node again for traversing
        traversal.add_node(None, target_addr);

        let result = traversal.unqueried.len();
        assert_eq!(
            0, result,
            "expected the traversed node address to have been ignored"
        );
    }
}
