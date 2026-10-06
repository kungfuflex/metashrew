//! v9.0.5-rc.15: a block AHEAD of the committed tip must not burn the retry
//! budget.
//!
//! Companion to `snapshot_path_stale_retry_test.rs`, which pins the OPPOSITE
//! direction. That test covers `height < current_height` — a block already
//! applied by a racing path — and the 2026-05 staleness guard fixed it.
//!
//! This test covers `height > committed_tip`, which nothing guarded and which
//! wedged mainnet three times (INCIDENT-REORG-WEDGE-20260816, -20260824, and
//! the 948955 event recorded in the sibling test).
//!
//! The distinction that matters, and the reason the earlier guard did not
//! catch this: the staleness guard compares the requested height against the
//! engine's IN-MEMORY `current_height`. The rejection actually raised by
//! `commit_atomic` compares it against the ON-DISK tip. When the cursor itself
//! has drifted ahead of the disk — which is exactly what a reorg does — the two
//! agree with each other and disagree with storage, so the guard sees nothing
//! wrong. Nothing in the codebase compared the cursor against the disk.
//!
//! Production shape (2026-08-24, `rockshrew-a-0`):
//!
//!   PROCESSOR: Failed block 963856 after 2577.178875719s
//!     commit_atomic at height 963856 exceeded max-retry budget (30 attempts):
//!     out-of-order commit rejected: attempted height 963856 but current tip
//!     is 963852 (commit_atomic only accepts height == tip + 1)
//!
//! ~43 minutes per block, re-learning on attempt 30 what was knowable on
//! attempt 1, while the fetcher marched further ahead and widened the gap.
//!
//! The invariant pinned here: when the requested height is ahead of the
//! committed tip, `process_block` must return the out-of-order error
//! PROMPTLY — not after 30 backoff rounds. Prompt return is what lets the
//! caller route it to `handle_reorg`, which rewinds the cursor.
//!
//! Without the rc.15 fix this test fails on the elapsed-time assertion: the
//! bounded retry loop takes minutes to give up.

use metashrew_sync::snapshot::SyncMode;
use metashrew_sync::snapshot_sync::SnapshotMetashrewSync;
use metashrew_sync::{
    MockBitcoinNode, MockRuntime, MockStorage, SnapshotSyncEngine, StorageAdapter, SyncConfig,
};
use std::time::{Duration, Instant};
use tokio::time::timeout;

async fn warm_storage_to_tip(storage: &mut MockStorage, tip: u32) {
    for h in 0..=tip {
        let bh = vec![h as u8; 32];
        let sr = vec![0u8; 32];
        storage.commit_atomic(h, &bh, &sr, &[]).await.unwrap();
    }
}

fn test_config() -> SyncConfig {
    SyncConfig {
        start_block: 0,
        exit_at: None,
        pipeline_size: None,
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        // Skip startup-heal: we hand-warm storage, so there is nothing to
        // validate against bitcoind (which would need a richer mock).
        enable_startup_heal: false,
    }
}

/// Storage tip is 100; we drive block 104 (tip + 4), the gap a reorg opens.
///
/// Pre-fix: 30 retries with exponential backoff — minutes.
/// Post-fix: returns the out-of-order error on the first attempt.
#[tokio::test]
async fn block_ahead_of_tip_fails_fast_instead_of_burning_the_retry_budget() {
    // ---- setup ----
    let mut storage = MockStorage::new();
    warm_storage_to_tip(&mut storage, 100).await;

    let node = MockBitcoinNode::new();
    // Give the node the block we will request, so the failure under test is the
    // commit rejection and not a missing-block lookup.
    node.add_block(104, vec![104u8; 32], vec![0u8; 80]);

    let runtime = MockRuntime::new();
    let mut sync =
        SnapshotMetashrewSync::new(node, storage, runtime, test_config(), SyncMode::Normal);
    sync.init().await;

    // The cursor sits at tip + 1 after init. Crucially we do NOT move it: the
    // point is that the requested height is ahead of the DISK, which the
    // staleness guard (height < current_height) cannot see.
    assert_eq!(
        sync.current_height(),
        101,
        "setup invariant: post-init cursor should be storage tip + 1"
    );

    // ---- act ----
    let block_data = vec![0u8; 80];
    let started = Instant::now();
    let res = timeout(
        Duration::from_secs(20),
        sync.process_block_with_snapshots(104, &block_data),
    )
    .await;
    let elapsed = started.elapsed();

    // ---- assert ----
    assert!(
        res.is_ok(),
        "process_block did not return within 20s for a height ahead of the \
         committed tip (tip=100, requested=104) — the retry loop is still \
         spending its full budget on a deterministic rejection"
    );

    let inner = res.unwrap();
    assert!(
        inner.is_err(),
        "committing a block ahead of the tip must be an error, not silently accepted"
    );
    let msg = format!("{}", inner.unwrap_err());
    assert!(
        msg.contains("out-of-order commit rejected"),
        "expected the out-of-order rejection to be surfaced verbatim so the \
         result loop can route it to handle_reorg; got: {msg}"
    );

    // The real regression. The bounded loop would take minutes here; a
    // fail-fast path returns essentially immediately. 5s is far below the
    // pre-fix cost and far above any plausible mock latency.
    assert!(
        elapsed < Duration::from_secs(5),
        "out-of-order rejection took {elapsed:?} — it must fail fast. Retrying \
         cannot change the gap between the requested height and the committed \
         tip, and every retry delays the resync signal that would fix it."
    );
}

// ---------------------------------------------------------------------------
// v9.0.5-rc.16: failing fast is only half the fix — the cursor must REWIND.
// ---------------------------------------------------------------------------

/// Node whose hashes match what `warm_storage_to_tip` wrote, up to `tip`.
fn node_matching_storage(tip: u32) -> MockBitcoinNode {
    let node = MockBitcoinNode::new();
    for h in 0..=tip {
        node.add_block(h, vec![h as u8; 32], vec![0u8; 80]);
    }
    node
}

/// Cursor ahead of the on-disk tip with no real reorg: the shape
/// `handle_reorg(failed_height, ..)` alone cannot fix, because it skips the
/// gap heights (no local hash), finds a matching ancestor, and returns
/// `failed_height` unchanged.
#[tokio::test]
async fn resync_rewinds_cursor_ahead_of_disk_without_a_reorg() {
    let mut storage = MockStorage::new();
    warm_storage_to_tip(&mut storage, 100).await;
    let sync = SnapshotMetashrewSync::new(
        node_matching_storage(110),
        storage,
        MockRuntime::new(),
        test_config(),
        SyncMode::Normal,
    );
    sync.init().await;
    assert_eq!(sync.current_height(), 101);

    // Disk falls behind the cursor: tip 96, cursor still 101.
    sync.storage().write().await.rollback_to_height(96).await.unwrap();

    // Pre-rc.16 recovery: no rewind.
    let via_handle_reorg = metashrew_sync::sync::handle_reorg(
        101,
        sync.node().clone(),
        sync.storage().clone(),
        sync.runtime().clone(),
        &sync.config,
    )
    .await
    .unwrap();
    assert_eq!(
        via_handle_reorg, 101,
        "precondition: handle_reorg from the failed height does not close the gap"
    );

    let next = sync.resync_cursor_to_storage_tip().await.unwrap();
    assert_eq!(next, 97, "resync must anchor at on-disk tip + 1");
    assert_eq!(sync.current_height(), 97, "resync must store the rewound cursor");
}

/// A real reorg below the on-disk tip: resync still rolls back to the
/// common ancestor, not just to tip + 1.
#[tokio::test]
async fn resync_still_rolls_back_a_real_reorg_below_the_tip() {
    let mut storage = MockStorage::new();
    warm_storage_to_tip(&mut storage, 100).await;
    let node = node_matching_storage(110);
    // Remote chain diverged at 99 and 100.
    node.add_block(99, vec![0xAA; 32], vec![0u8; 80]);
    node.add_block(100, vec![0xBB; 32], vec![0u8; 80]);
    let sync = SnapshotMetashrewSync::new(
        node,
        storage,
        MockRuntime::new(),
        test_config(),
        SyncMode::Normal,
    );
    sync.init().await;

    let next = sync.resync_cursor_to_storage_tip().await.unwrap();
    assert_eq!(next, 99, "common ancestor is 98, so resume at 99");
    assert_eq!(sync.current_height(), 99);
    assert_eq!(sync.get_height().await.unwrap(), 98, "storage rolled back to ancestor");
}
