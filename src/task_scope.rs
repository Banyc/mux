//! Actively-owned task scopes.
//!
//! Dropping a [`JoinSet`] aborts its children but hides completed values
//! and panics. [`abort_and_reap`] shuts a scope down explicitly: every
//! child is aborted, every completion is joined, cancellation
//! ([`tokio::task::JoinError::is_cancelled`]) is treated as the owner's
//! expected request, and anything else — a panic that beat the abort —
//! is re-raised across the boundary.

use tokio::task::JoinSet;

/// Abort every child and reap the scope, swallowing only cancellation.
///
/// A child that panicked before (or instead of) observing the abort still
/// surfaces its panic through the reap, so a racing panic can never be
/// silently downgraded to a clean shutdown.
pub(crate) async fn abort_and_reap<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if joined
            .as_ref()
            .is_err_and(tokio::task::JoinError::is_cancelled)
        {
            continue;
        }
        let _ = joined.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // An owner-requested shutdown aborts and drains every child; the
    // resulting cancellation is expected, not an error.
    #[tokio::test]
    async fn cancellation_requested_by_the_owner_is_expected() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async {
            std::future::pending::<()>().await;
        });
        tasks.spawn(async {
            std::future::pending::<()>().await;
        });
        abort_and_reap(&mut tasks).await;
        assert!(
            tasks.is_empty(),
            "abort_and_reap must drain every cancelled child"
        );
    }

    // A child panic that beat the owner's abort is re-raised by the reap
    // with its original payload instead of being swallowed.
    #[tokio::test]
    #[should_panic(expected = "intentional child panic that beat cancellation")]
    async fn a_child_panic_that_beat_cancellation_still_cascades() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async {
            panic!("intentional child panic that beat cancellation");
        });
        // Let the child run so its panic is recorded before the reap.
        tokio::task::yield_now().await;
        abort_and_reap(&mut tasks).await;
    }
}
