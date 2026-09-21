//! The mux layer's transport-free testing kit: wrapping an already-connected
//! reliable byte-stream pair in a `mux` client and supervising its session.
//!
//! Every helper here takes a byte-stream pair (or a [`crate::StreamOpener`])
//! and drives `mux` itself, so the module needs nothing from a transport.
//! The transport-mediated scaffolding — the `mux`-over-`rtp` servers, sinks,
//! transient connects and probe plumbing — lives in the cooperation crate's
//! kit, which is the one place that sees both layers together. Generic
//! scenario helpers live in the `netem-test` harness kit (`netem_test::kit`,
//! behind its `test-kit` feature). Imports only ever go downward (mux kit →
//! harness kit), so `netem-test` stays a leaf.
//!
//! The operator's product constitution is stated in `GATE.md`
//! ("Performance"); the topology-level gates that need both layers are owned
//! by the crate that owns the dual-lane topology.

use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;

use super::stats::MuxSessionProgress;
use netem_test::kit::{
    TestScope, TestTask, TestTaskSubmitter, submit_test_task, submit_test_task_required,
};

/// Shared core for [`mux_client_connect`] and its `_via` variant: wraps the
/// reliable byte-stream pair in a `mux` client and hands the supervision
/// drain future to `spawn_required` (either a [`TestScope`] spawn or the
/// bounded reaper submission).
fn mux_client_connect_core<R, W>(
    spawn_required: impl FnOnce(&'static str, TestTask),
    read: R,
    write: W,
) -> crate::StreamOpener
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let config = crate::MuxConfig {
        initiation: crate::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: false,
    };
    let mut spawner = JoinSet::new();
    let (opener, _accepter) = crate::spawn_mux_no_reconnection(read, write, config, &mut spawner);
    spawn_required(
        "mux client session",
        Box::pin(async move {
            if let Some(result) = spawner.join_next().await {
                let err = result.unwrap();
                panic!("mux client session ended before the test body: {err:?}");
            }
        }),
    );
    opener
}

/// Wrap a reliable byte-stream pair in a `mux` client and return the stream
/// opener. The mux supervision `JoinSet` is drained by a required scope task:
/// the session must survive the whole test body, a panicked supervision task
/// surfaces immediately, and the session ending before the body completes is
/// a panic. Callers that intentionally end the session mid-body must use an
/// ordinary-spawn drain instead.
pub fn mux_client_connect<R, W>(tasks: &mut TestScope, read: R, write: W) -> crate::StreamOpener
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    mux_client_connect_core(|name, fut| tasks.spawn_required(name, fut), read, write)
}

/// [`mux_client_connect`] through the bounded task-submission handle, for use
/// inside [`TestScope::run`] bodies where `&mut TestScope` is unavailable.
/// The supervision drain is submitted as required through the handle.
pub fn mux_client_connect_via<R, W>(
    tx: &TestTaskSubmitter,
    read: R,
    write: W,
) -> crate::StreamOpener
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    mux_client_connect_core(
        |name, fut| submit_test_task_required(tx, name, fut),
        read,
        write,
    )
}

/// Wrap a frame-preserving reliable stream pair (from the transport layer's
/// frame-delivery connect helper) in a `mux` client configured for
/// `frame_reassembly`, matching the deployment's frame-delivery path, and
/// return the stream opener.
///
/// Unlike [`mux_client_connect_via`], the supervision drain is submitted as a
/// non-required background task: a frame-delivery lane exchanges a normal FIN
/// at teardown before the measurement body finishes, so the session can
/// complete normally and a required drain would panic on that early end. A
/// panicked supervision task still surfaces at scope end.
pub fn mux_client_connect_frame_delivery_via<R, W>(
    tx: &TestTaskSubmitter,
    read: R,
    write: W,
) -> crate::StreamOpener
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let config = crate::MuxConfig {
        initiation: crate::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    };
    let mut spawner = JoinSet::new();
    let (opener, _accepter) = crate::spawn_mux_no_reconnection(read, write, config, &mut spawner);
    submit_test_task(
        tx,
        Box::pin(async move {
            if let Some(result) = spawner.join_next().await
                && let Err(err) = result
            {
                panic!("mux client frame-delivery session supervision failed: {err:?}");
            }
        }),
    );
    opener
}

/// Wrap a `mux` client session whose supervision drain is an ordinary spawn
/// rather than `spawn_required`. JoinErrors are unwrapped so a panicked
/// supervision task still fails the test; a `MuxError` session-end is the
/// expected teardown here, not a failure. The first terminal mux error is
/// latched into the returned [`MuxSessionProgress`].
pub fn mux_client_connect_transient<R, W>(
    task_tx: &netem_test::kit::TestTaskSubmitter,
    read: R,
    write: W,
) -> (crate::StreamOpener, std::sync::Arc<MuxSessionProgress>)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let config = crate::MuxConfig {
        initiation: crate::Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: false,
    };
    let mut spawner = tokio::task::JoinSet::new();
    let (opener, _accepter) = crate::spawn_mux_no_reconnection(read, write, config, &mut spawner);
    let progress = std::sync::Arc::new(MuxSessionProgress::new());
    // Transient drain (see doc comment): unwrap JoinErrors so panics
    // surface; a `MuxError` session-end ends the drain normally. Submitted
    // through the already-active bounded outer submitter instead of an
    // unpolled body-local scope, so a panicked supervision task fails the
    // test immediately rather than disappearing when the scope is dropped.
    netem_test::kit::submit_test_task(
        task_tx,
        Box::pin({
            let progress = std::sync::Arc::clone(&progress);
            async move {
                if let Some(result) = spawner.join_next().await {
                    let error = result.unwrap();
                    progress.record_error(&error);
                }
            }
        }),
    );
    (opener, progress)
}
