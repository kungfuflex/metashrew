//! v9.0.5-rc.16: an out-of-order commit rejection must rewind the fetcher.
//!
//! rc.15 made `process_block` fail fast on "out-of-order commit rejected" and
//! taught the metashrew-sync result loop to treat it as a reorg signal. But
//! rockshrew-mono runs its own fetcher/processor/indexer tasks; its
//! `BlockResult::Error` arm only matched the two chain-discontinuity strings,
//! so the error fell through to sleep-and-continue, `last_sent_height` was
//! never reset and the engine cursor never lowered. The fetcher kept
//! computing `max(last_sent + 1, processor_tip)` — ahead of disk — until a
//! restart ran startup-heal.
//!
//! These tests drive the same three pieces the indexer arm composes:
//! `is_resync_error`, `SnapshotMetashrewSync::resync_cursor_to_storage_tip`,
//! and `compute_next_fetch_range` after the `LAST_SENT_UNSET` reset.

use crate::{compute_next_fetch_range, is_resync_error, LAST_SENT_UNSET};
use metashrew_sync::snapshot::SyncMode;
use metashrew_sync::snapshot_sync::SnapshotMetashrewSync;
use metashrew_sync::{
    MockBitcoinNode, MockRuntime, MockStorage, SnapshotSyncEngine, StorageAdapter, SyncConfig,
};

#[test]
fn out_of_order_rejection_is_a_resync_error() {
    // Verbatim production shape (2026-08-24, rockshrew-a-0), as surfaced by
    // the rc.15 fail-fast path.
    let prod = "Storage error: commit_atomic: out-of-order commit rejected: attempted \
                height 963856 but current tip is 963852 (commit_atomic only accepts \
                height == tip + 1)";
    assert!(is_resync_error(prod));
    assert!(is_resync_error(
        "Block does not connect to previous block - possible reorg or chain inconsistency"
    ));
    assert!(is_resync_error("CHAIN DISCONTINUITY at 100"));
    // Transient storage failures are not resync signals.
    assert!(!is_resync_error("commit_atomic: Database error: Too many open files"));
}

#[tokio::test]
async fn out_of_order_rejection_rewinds_fetcher_to_disk_tip() {
    let mut storage = MockStorage::new();
    for h in 0..=100u32 {
        storage.commit_atomic(h, &[h as u8; 32], &[0u8; 32], &[]).await.unwrap();
    }
    let node = MockBitcoinNode::new();
    for h in 0..=110u32 {
        node.add_block(h, vec![h as u8; 32], vec![0u8; 80]);
    }
    let config = SyncConfig {
        start_block: 0,
        exit_at: None,
        pipeline_size: None,
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };
    let mut sync = SnapshotMetashrewSync::new(node, storage, MockRuntime::new(), config, SyncMode::Normal);
    sync.init().await;

    // Cursor drifts ahead of disk: disk tip 96, cursor 101, and the fetcher
    // has already enqueued through 104.
    sync.storage().write().await.rollback_to_height(96).await.unwrap();
    let last_sent: i64 = 104;
    assert_eq!(
        compute_next_fetch_range(sync.current_height(), last_sent, 10, 110),
        Some((105, 111)),
        "precondition: without recovery the fetcher keeps moving away from disk"
    );

    // The processor rejects the next block it is handed.
    let err = sync
        .process_block_with_snapshots(101, &[0u8; 80])
        .await
        .expect_err("block ahead of disk tip must be rejected");
    let err_str = err.to_string();
    assert!(is_resync_error(&err_str), "indexer arm must match: {err_str}");

    // Indexer arm: resync the engine, reset the fetcher watermark.
    let rollback = sync.resync_cursor_to_storage_tip().await.unwrap();
    assert_eq!(rollback, 97);
    let last_sent = LAST_SENT_UNSET;

    assert_eq!(
        compute_next_fetch_range(sync.current_height(), last_sent, 10, 110),
        Some((97, 107)),
        "after recovery the fetcher must restart at on-disk tip + 1"
    );

    // And the block the fetcher now sends commits.
    sync.process_block_with_snapshots(97, &[0u8; 80]).await.unwrap();
    assert_eq!(sync.get_height().await.unwrap(), 97);
}
