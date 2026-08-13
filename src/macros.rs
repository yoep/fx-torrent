/// Create a new channel for sending and receiving messages between torrent tasks.
///
/// This macro supports:
/// - `channel!()` for a bounded (backpressure) channel with default capacity `256`.
/// - `channel!(N)` for a bounded (backpressure) channel with capacity `N`.
macro_rules! channel {
    () => {{
        channel!(256)
    }};
    ($limit:expr) => {{
        let limit: usize = $limit;
        crate::channel::channel(limit)
    }};
}

/// Spawn a (conditionally) instrumented tokio task.
///
/// # Example Usage
///
/// ```rust,compile_fail
/// spawn!("Struct::fn", async {})
/// ```
macro_rules! spawn {
    ($name:expr, $future:expr) => {{
        let future = $future;

        #[cfg(feature = "tracing")]
        let future = {
            use tracing::Instrument;
            future.instrument(tracing::info_span!($name))
        };

        tokio::spawn(future)
    }};
}
