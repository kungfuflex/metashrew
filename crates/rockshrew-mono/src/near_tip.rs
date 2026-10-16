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

use crate::{
    is_resync_error, near_tip_min_send_height, near_tip_should_drop, BlockData, LAST_SENT_UNSET,
};
use log::{debug, info, warn};
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
pub(crate) async fn near_tip_fetch_step<N, S, R>(
    engine: &SnapshotMetashrewSync<N, S, R>,
    _last_sent_height: &AtomicI64,
    last_sent_snapshot: i64,
) -> NearTipFetch
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    // v9.0.5-rc.13: pass min_send_height as the fetch floor so
    // get_next_block_data ALWAYS returns a block at height >= last_sent + 1
    // (see `run()` for the history of this floor).
    let min_send_height = near_tip_min_send_height(last_sent_snapshot);
    match engine
        .get_next_block_data_with_floor(Some(min_send_height))
        .await
    {
        Ok(Some((height, block_data, block_hash))) => {
            if near_tip_should_drop(last_sent_snapshot, height) {
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
/// Returns the height the engine cursor was re-anchored at, if it was.
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
    // Chain validation errors and out-of-order commits both mean the
    // cursor no longer matches storage — resync.
    if !is_resync_error(error_str) {
        return Ok(None);
    }
    warn!("Chain discontinuity at height {}. Triggering reorg.", height);

    // v9.0.5-rc.16: re-anchor the engine cursor to the on-disk tip before
    // reorg detection, then reset the fetcher's watermark so it resumes
    // from the processor's tip (store, not fetch_min: a stale last_sent
    // must never survive a rollback).
    let rollback_height = engine.resync_cursor_to_storage_tip().await?;
    info!("Rolled back to height {}. Resuming sync.", rollback_height);
    last_sent_height.store(LAST_SENT_UNSET, Ordering::SeqCst);
    warn!(
        "FETCHER: last_sent_height reset to UNSET after rollback to {} — fetcher will resume from processor_tip",
        rollback_height
    );
    Ok(Some(rollback_height))
}
