//! Regression tests for the 2026-10-14 alspo indexer commit stall.
//!
//! What happened in production (alspo-subfrostamm-0, alspo-btcusd-0 at
//! 971344; alspo-cbtcusd-0 and alspo-cfire-0 at 966499 — every one a real
//! 1-block mainnet reorg, see `getchaintips` valid-fork 971345 / 966500):
//!
//! 1. Block N (971345) committed. A competing block won, the chain
//!    reorganised, and the fetcher's near-tip reorg check
//!    (`get_next_block_data_with_floor`) rolled storage back to N-1.
//! 2. The rc.13 floor (`last_sent + 1`) then overrode the rolled-back
//!    cursor, so N was never fetched again; the fetcher sent N+1, N+2, …
//! 3. N+1 had no stored parent hash (the rollback deleted it), so the
//!    continuity check passed, and `commit_atomic` rejected it
//!    out-of-order against tip N-1. That rejection was retried 30 times
//!    (~11 min), surfaced as a generic error, and nothing rewound the
//!    cursor: N+2 failed the same way, and so on for 19 hours until the
//!    pod was deleted.
//!
//! These tests drive the real `SnapshotMetashrewSync` with mock adapters
//! and real Bitcoin headers so the continuity check runs for real.

use bitcoin::block::{Header, Version};
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, TxMerkleNode};
use metashrew_sync::snapshot::SyncMode;
use metashrew_sync::snapshot_sync::SnapshotMetashrewSync;
use metashrew_sync::{MockBitcoinNode, MockRuntime, MockStorage, StorageAdapter, SyncConfig};
use std::time::Duration;
use tokio::time::timeout;

type Engine = SnapshotMetashrewSync<MockBitcoinNode, MockStorage, MockRuntime>;

/// (height, display-order hash bytes, serialized block)
type B = (u32, Vec<u8>, Vec<u8>);

fn mk_block(prev: BlockHash, salt: u32) -> (BlockHash, Vec<u8>) {
    let block = Block {
        header: Header {
            version: Version::from_consensus(0x2000_0000),
            prev_blockhash: prev,
            merkle_root: TxMerkleNode::all_zeros(),
            time: 1_700_000_000 + salt,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: salt,
        },
        txdata: vec![],
    };
    (block.block_hash(), bitcoin::consensus::serialize(&block))
}

fn display(h: &BlockHash) -> Vec<u8> {
    let mut v = h.to_byte_array().to_vec();
    v.reverse();
    v
}

/// Build blocks `from..=to` on top of `prev`. `salt` makes forks differ.
fn chain(mut prev: BlockHash, from: u32, to: u32, salt: u32) -> (Vec<B>, BlockHash) {
    let mut out = Vec::new();
    for h in from..=to {
        let (hash, data) = mk_block(prev, h * 1000 + salt);
        out.push((h, display(&hash), data));
        prev = hash;
    }
    (out, prev)
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

/// Chain A 0..=10 indexed, then a 1-block reorg replaces 10 and extends to 12.
/// Returns the engine (cursor 11, storage tip 10) and chain B 10'..=12'.
async fn indexed_then_reorged() -> (Engine, MockStorage, Vec<B>) {
    let node = MockBitcoinNode::new();
    let storage = MockStorage::new();
    let (a, _) = chain(BlockHash::all_zeros(), 0, 10, 1);
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
        engine.process_block(*h, data.clone(), hash.clone()).await.unwrap();
    }
    assert_eq!(storage.get_indexed_height().await.unwrap(), 10);
    assert_eq!(engine.current_height(), 11);

    // Competing block at 10 wins; parent is A9.
    let a9 = {
        let block: Block = bitcoin::consensus::deserialize(&a[9].2).unwrap();
        block.block_hash()
    };
    let (b, _) = chain(a9, 10, 12, 2);
    node.simulate_reorg(10, b.iter().cloned().collect());
    (engine, storage, b)
}

/// Step 1+2 of the incident. The fetcher had already sent 10, so it asks
/// with floor 11. The reorg check rolls storage back to 9; the engine must
/// hand back 10 (the block that re-extends the tip) and announce the
/// rollback, not 11.
#[tokio::test]
async fn floor_does_not_skip_the_rollback_height() {
    let (engine, storage, _b) = indexed_then_reorged().await;
    let epoch = engine.rollback_epoch();

    let next = engine
        .get_next_block_data_with_floor(Some(11))
        .await
        .unwrap()
        .expect("node has blocks past the rollback");

    assert_eq!(storage.get_indexed_height().await.unwrap(), 9, "reorg check rolled back");
    assert_eq!(
        next.0, 10,
        "after a real rollback the floor (last_sent+1 = 11) must not apply: \
         fetching 11 leaves 10 missing forever and every later commit is rejected"
    );
    assert_ne!(engine.rollback_epoch(), epoch, "rollback must be visible to the fetcher");
}

/// Step 3 of the incident. A block above the on-disk tip must fail fast
/// (the out-of-order rejection is deterministic), not after a 30-attempt
/// ~11-minute retry budget, and a resync must re-anchor at tip + 1.
#[tokio::test]
async fn out_of_order_fails_fast_and_resync_reanchors() {
    let (engine, storage, b) = indexed_then_reorged().await;
    storage.clone().rollback_to_height(9).await.unwrap();

    let (h, hash, data) = b[1].clone(); // 11', no stored parent at 10
    let res = timeout(Duration::from_secs(5), engine.process_block(h, data, hash)).await;
    let inner = res.expect("out-of-order commit was retried instead of surfacing immediately");
    let err = inner.expect_err("commit above the on-disk tip must not succeed");
    assert!(format!("{}", err).contains("out-of-order commit rejected"), "{}", err);
    assert_eq!(storage.get_indexed_height().await.unwrap(), 9, "nothing committed");

    let epoch = engine.rollback_epoch();
    let next = engine.resync_cursor_to_storage_tip().await.unwrap();
    assert_eq!(next, 10);
    assert_eq!(engine.current_height(), 10);
    assert_ne!(engine.rollback_epoch(), epoch);
}

/// The whole loop, driven the way rockshrew-mono drives it (fetcher
/// watermark + floor, processor, resync on any failure). Before the fix
/// this never got past storage tip 9; with it, it reaches the new tip.
#[tokio::test]
async fn fetcher_processor_loop_recovers_from_one_block_reorg() {
    let (engine, storage, _b) = indexed_then_reorged().await;

    // The fetcher's own watermark: it had already sent 10 before the reorg.
    let mut last_sent: i64 = 10;
    let mut failures = 0u32;
    for _ in 0..40 {
        if storage.get_indexed_height().await.unwrap() == 12 {
            break;
        }
        let floor = if last_sent < 0 { 0 } else { last_sent as u32 + 1 };
        let epoch = engine.rollback_epoch();
        let Some((h, data, hash)) = engine.get_next_block_data_with_floor(Some(floor)).await.unwrap()
        else {
            break;
        };
        if engine.rollback_epoch() != epoch {
            last_sent = -1;
        } else if (h as i64) <= last_sent {
            continue; // near-tip duplicate
        }
        last_sent = last_sent.max(h as i64);
        if engine.process_block(h, data, hash).await.is_err() {
            failures += 1;
            engine.resync_cursor_to_storage_tip().await.unwrap();
            last_sent = -1;
        }
    }
    assert_eq!(
        storage.get_indexed_height().await.unwrap(),
        12,
        "indexer must reach the new tip after a 1-block reorg without a restart \
         ({} failed blocks on the way)",
        failures
    );
}
