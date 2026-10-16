//! The near-tip fetcher step and the indexer's answer to a failed block,
//! taken out of `run()` so tests drive exactly the code production runs.
//!
//! `run()` wires three tasks together: the fetcher (sends blocks into the
//! prefetch channel), the processor (`process_block` on each one) and the
//! indexer (reads the processor's results and resyncs on failure). Near the
//! tip the fetcher goes block by block through
//! `get_next_block_data_with_floor`, which is also where reorgs are detected
//! and storage is rolled back. The functions here are those two decision
//! points; `run()` only adds the channel, the sleeps and the logging around
//! them.

use crate::{near_tip_min_send_height, near_tip_should_drop, BlockData, LAST_SENT_UNSET};
use log::{debug, warn};
use metashrew_sync::{
    BitcoinNodeAdapter, RuntimeAdapter, SnapshotMetashrewSync, StorageAdapter, SyncError,
};
use std::sync::atomic::{AtomicI64, Ordering};

/// What one near-tip fetcher iteration produced.
#[derive(Debug)]
pub(crate) enum NearTipFetch {
    /// Send this block to the processor. After a successful send the caller
    /// bumps the watermark with [`record_sent`].
    Send(BlockData),
    /// A duplicate of a block already enqueued; retry shortly.
    Duplicate,
    /// Nothing new on the node yet.
    Idle,
    /// The node or the reorg check failed; retry later.
    Failed(SyncError),
}

/// One near-tip fetcher iteration. `last_sent_snapshot` is the watermark the
/// caller read at the top of its loop (it chose the near-tip path with it).
///
/// v9.1.0-rc.3: if the engine rolled storage back during this call (its
/// reorg check found the tip orphaned) or a resync re-anchored the cursor
/// meanwhile, `last_sent_height` points past what storage holds. It is
/// reset here, before the block at the rolled-back height is handed out,
/// and that block is never treated as a duplicate.
pub(crate) async fn near_tip_fetch_step<N, S, R>(
    engine: &SnapshotMetashrewSync<N, S, R>,
    last_sent_height: &AtomicI64,
    last_sent_snapshot: i64,
) -> NearTipFetch
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    // v9.0.5-rc.13: the floor (`last_sent + 1`) keeps get_next_block_data
    // from re-fetching what is already enqueued while the processor lags
    // (j stuck at 951049, 2026-05-26). The engine ignores it after a real
    // rollback, which it reports through `rollback_epoch`.
    let min_send_height = near_tip_min_send_height(last_sent_snapshot);
    let epoch_before = engine.rollback_epoch();
    let result = engine
        .get_next_block_data_with_floor(Some(min_send_height))
        .await;
    let rolled_back = engine.rollback_epoch() != epoch_before;
    if rolled_back {
        warn!(
            "FETCHER: storage rolled back (reorg) — last_sent {} is past the on-disk tip; watermark reset, resuming at height {}",
            last_sent_snapshot,
            engine.current_height(),
        );
        last_sent_height.store(LAST_SENT_UNSET, Ordering::SeqCst);
    }
    match result {
        Ok(Some((height, block_data, block_hash))) => {
            if !rolled_back && near_tip_should_drop(last_sent_snapshot, height) {
                debug!(
                    "FETCHER: dropping near-tip duplicate height={} (last_sent={} min_send={})",
                    height, last_sent_snapshot, min_send_height,
                );
                return NearTipFetch::Duplicate;
            }
            NearTipFetch::Send(BlockData {
                height,
                block_data,
                block_hash,
            })
        }
        Ok(None) => NearTipFetch::Idle,
        Err(e) => NearTipFetch::Failed(e),
    }
}

/// Bump the fetcher watermark after a successful send. `fetch_max` so a
/// concurrent rollback-reset can't be clobbered by a stale send that
/// happened to win the race.
pub(crate) fn record_sent(last_sent_height: &AtomicI64, height: u32) {
    last_sent_height.fetch_max(height as i64, Ordering::SeqCst);
}

/// The indexer task's answer to a block the processor could not commit.
/// Returns the height the engine cursor was re-anchored at.
///
/// v9.1.0-rc.3: EVERY failure resyncs, not only the chain-discontinuity /
/// out-of-order ones (`crate::is_resync_error` is kept for logging). The
/// block did not commit, and the fetcher runs ahead of the processor, so
/// whatever it queued after it has no parent on disk. Re-anchoring at the
/// on-disk tip + 1 (the startup heal's rule, `resync_cursor_to_storage_tip`)
/// and clearing the watermark (store, not fetch_min: a stale last_sent must
/// never survive a rollback) makes the next block attempted `indexed + 1`.
/// Same rule as the alspo line's commit-stall fix (metashrew PR #63).
pub(crate) async fn recover_from_failed_block<N, S, R>(
    engine: &SnapshotMetashrewSync<N, S, R>,
    last_sent_height: &AtomicI64,
    height: u32,
    error_str: &str,
) -> Result<Option<u32>, SyncError>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    if crate::is_resync_error(error_str) {
        warn!(
            "Chain discontinuity at height {}. Triggering reorg.",
            height
        );
    }
    let next_height = engine.resync_cursor_to_storage_tip().await?;
    last_sent_height.store(LAST_SENT_UNSET, Ordering::SeqCst);
    warn!(
        "resync after failed block {}: cursor re-anchored at {} (on-disk tip + 1); fetcher watermark reset",
        height, next_height
    );
    Ok(Some(next_height))
}
