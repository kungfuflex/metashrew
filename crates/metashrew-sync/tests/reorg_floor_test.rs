//! Engine-level regression tests for the v9.1.0-rc.1/rc.2 reorg stall: the
//! fetch floor (`last_sent + 1`, rc.13) must not apply after the engine's
//! own reorg check rolled storage back, and the rollback must be visible to
//! the caller through `rollback_epoch`.
//!
//! The full fetcher/processor loop on RocksDB is driven in
//! crates/rockshrew-mono/src/tests/reorg_stall_test.rs.

use bitcoin::block::{Header, Version};
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, TxMerkleNode};
use metashrew_sync::snapshot::SyncMode;
use metashrew_sync::snapshot_sync::SnapshotMetashrewSync;
use metashrew_sync::{
    MetashrewSync, MockBitcoinNode, MockRuntime, MockStorage, StorageAdapter, SyncConfig,
};
use std::time::Duration;
use tokio::time::timeout;

type Engine = SnapshotMetashrewSync<MockBitcoinNode, MockStorage, MockRuntime>;
type B = (u32, Vec<u8>, Vec<u8>);

fn chain(mut prev: BlockHash, from: u32, to: u32, salt: u32) -> Vec<B> {
    let mut out = Vec::new();
    for h in from..=to {
        let block = Block {
            header: Header {
                version: Version::from_consensus(0x2000_0000),
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::all_zeros(),
                time: 1_700_000_000 + h,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: salt,
            },
            txdata: vec![],
        };
        prev = block.block_hash();
        let mut d = prev.to_byte_array().to_vec();
        d.reverse();
        out.push((h, d, bitcoin::consensus::serialize(&block)));
    }
    out
}

fn hash_of(b: &B) -> BlockHash {
    let block: Block = bitcoin::consensus::deserialize(&b.2).unwrap();
    block.block_hash()
}

fn config() -> SyncConfig {
    SyncConfig {
        start_block: 0,
        exit_at: None,
        pipeline_size: None,
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    }
}

/// Chain A 0..=10 indexed (cursor 11), then the chain from `from` is
/// replaced by a competing branch up to `to`.
async fn indexed_then_reorged(from: u32, to: u32) -> (Engine, MockStorage, Vec<B>) {
    let node = MockBitcoinNode::new();
    let storage = MockStorage::new();
    let a = chain(BlockHash::all_zeros(), 0, 10, 1);
    for (h, hash, data) in &a {
        node.add_block(*h, hash.clone(), data.clone());
    }
    let engine = SnapshotMetashrewSync::new(
        node.clone(),
        storage.clone(),
        MockRuntime::new(),
        config(),
        SyncMode::Normal,
    );
    engine.init().await;
    for (h, hash, data) in &a {
        engine
            .process_block(*h, data.clone(), hash.clone())
            .await
            .unwrap();
    }
    assert_eq!(storage.get_indexed_height().await.unwrap(), 10);
    assert_eq!(engine.current_height(), 11);
    let b = chain(hash_of(&a[(from - 1) as usize]), from, to, 2);
    node.simulate_reorg(from, b.clone());
    (engine, storage, b)
}

/// testnet4 shape: the competing block replaces 10 and the node's tip stays
/// 10. The fetcher had sent 10, so it asks with floor 11. The engine must
/// roll back to 9 and hand back 10'. Before the fix it returned None (11
/// does not exist) on every poll until a new block arrived.
#[tokio::test]
async fn same_height_reorg_returns_the_replacement_block() {
    let (engine, storage, b) = indexed_then_reorged(10, 10).await;
    let epoch = engine.rollback_epoch();
    let next = engine
        .get_next_block_data_with_floor(Some(11))
        .await
        .unwrap()
        .expect("the replacement block at the rollback height must be returned, not None");
    assert_eq!(
        storage.get_indexed_height().await.unwrap(),
        9,
        "reorg check rolled back"
    );
    assert_eq!(next.0, 10);
    assert_eq!(next.2, b[0].1, "it is the new branch's block");
    assert_ne!(
        engine.rollback_epoch(),
        epoch,
        "rollback must be visible to the fetcher"
    );
}

/// The winning branch is already longer: the floor (11) must not skip 10'.
#[tokio::test]
async fn reorg_with_extension_does_not_skip_the_rollback_height() {
    let (engine, storage, _b) = indexed_then_reorged(10, 12).await;
    let next = engine
        .get_next_block_data_with_floor(Some(11))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(storage.get_indexed_height().await.unwrap(), 9);
    assert_eq!(
        next.0, 10,
        "fetching 11 leaves 10 missing and every later commit is rejected"
    );
}

/// Multi-block: 8..=10 replaced, floor 11 -> must restart at 8.
#[tokio::test]
async fn multi_block_reorg_restarts_at_the_fork_point() {
    let (engine, storage, _b) = indexed_then_reorged(8, 10).await;
    let next = engine
        .get_next_block_data_with_floor(Some(11))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(storage.get_indexed_height().await.unwrap(), 7);
    assert_eq!(next.0, 8);
}

/// Without a rollback the rc.13 floor still applies (no re-fetch of what
/// the fetcher already enqueued) and the epoch does not move.
#[tokio::test]
async fn floor_still_applies_without_a_rollback() {
    let (engine, _storage, _b) = indexed_then_reorged(11, 13).await; // extension only
    let epoch = engine.rollback_epoch();
    let next = engine
        .get_next_block_data_with_floor(Some(12))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.0, 12);
    assert_eq!(engine.rollback_epoch(), epoch);
}

/// An out-of-order block fails fast and a resync re-anchors at tip + 1 and
/// bumps the epoch.
#[tokio::test]
async fn out_of_order_fails_fast_and_resync_reanchors() {
    let (engine, storage, b) = indexed_then_reorged(10, 12).await;
    storage.clone().rollback_to_height(9).await.unwrap();
    let (h, hash, data) = b[1].clone(); // 11', no stored parent at 10
    let res = timeout(Duration::from_secs(5), engine.process_block(h, data, hash))
        .await
        .expect("out-of-order commit was retried instead of surfacing immediately");
    let err = res.expect_err("commit above the on-disk tip must not succeed");
    assert!(
        err.to_string().contains("out-of-order commit rejected"),
        "{}",
        err
    );
    let epoch = engine.rollback_epoch();
    assert_eq!(engine.resync_cursor_to_storage_tip().await.unwrap(), 10);
    assert_eq!(engine.current_height(), 10);
    assert_ne!(engine.rollback_epoch(), epoch);
}

/// The non-snapshot engine has the same floor; same rule.
#[tokio::test]
async fn metashrew_sync_same_height_reorg_returns_the_replacement_block() {
    let node = MockBitcoinNode::new();
    let storage = MockStorage::new();
    let a = chain(BlockHash::all_zeros(), 0, 10, 1);
    for (h, hash, data) in &a {
        node.add_block(*h, hash.clone(), data.clone());
    }
    let engine = MetashrewSync::new(node.clone(), storage.clone(), MockRuntime::new(), config());
    engine.init().await;
    for (h, hash, data) in &a {
        engine
            .process_block(*h, data.clone(), hash.clone())
            .await
            .unwrap();
    }
    // MetashrewSync's pipeline (not process_block) advances the cursor.
    engine
        .current_height
        .store(11, std::sync::atomic::Ordering::SeqCst);
    let b = chain(hash_of(&a[9]), 10, 10, 2);
    node.simulate_reorg(10, b.clone());
    let next = engine
        .get_next_block_data_with_floor(Some(11))
        .await
        .unwrap()
        .expect("replacement block, not None");
    assert_eq!(storage.get_indexed_height().await.unwrap(), 9);
    assert_eq!(next.0, 10);
}
