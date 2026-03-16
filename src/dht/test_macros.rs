/// Create a new DHT tracker server pair.
macro_rules! create_node_server_pair {
    () => {{
        create_node_server_pair!(crate::dht::NodeId::new(), crate::dht::NodeId::new())
    }};
    ($node_id1:expr, $node_id2:expr) => {{
        create_node_server_pair!($node_id1, $node_id2, true)
    }};
    ($node_id1:expr, $node_id2:expr, $enable_indexing:expr) => {{
        use crate::dht::DhtTracker;
        use crate::dht::NodeId;

        let node_id1: NodeId = $node_id1;
        let node_id2: NodeId = $node_id2;
        let enable_indexing: bool = $enable_indexing;

        let left_node = DhtTracker::builder()
            .node_id(node_id1)
            .enable_indexing(enable_indexing)
            .build()
            .await
            .unwrap();
        let right_node = DhtTracker::builder()
            .node_id(node_id2)
            .enable_indexing(enable_indexing)
            .build()
            .await
            .unwrap();

        (left_node, right_node)
    }};
}

/// Create a new DHT tracker context.
macro_rules! create_tracker_context {
    () => {{
        create_tracker_context!(crate::dht::NodeId::new())
    }};
    ($node_id:expr) => {{
        create_tracker_context!($node_id, false)
    }};
    ($node_id:expr, $enable_indexing:expr) => {{
        use crate::dht::DhtTracker;
        use crate::dht::ItemSignature;
        use crate::dht::NodeId;
        use crate::dht::TrackerContext;
        use std::sync::Arc;

        let id: NodeId = $node_id;
        let enable_indexing: bool = $enable_indexing;

        let socket = Arc::new(DhtTracker::bind_socket().await.unwrap());
        let socket_addr = socket.local_addr().unwrap();
        let item_verifier = ItemSignature::new().unwrap();

        TrackerContext::new(id, socket, socket_addr, enable_indexing, item_verifier)
    }};
}
