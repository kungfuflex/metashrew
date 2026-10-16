//! Reorg stall regression tests: after a rollback the next height must be
//! accepted with no restart.
//!
//! Production, testnet4 rockshrew-a (metashrew v9.1.0-rc.1, 2026-10-16):
//! every lost mining race was a 1-block reorg AT THE TIP. Our node had our
//! block N, the public chain had a different block N, and t4-tipfollow moved
//! our node over, so the node's tip height stayed N. The near-tip reorg
//! check rolled storage back to N-1, but the rc.13 fetch floor
//! (`last_sent + 1` = N+1) overrode the rolled-back cursor: N+1 does not
//! exist yet, so the fetcher idled and N was never fetched again. When N+1
//! finally arrived it was rejected out-of-order against tip N-1. The pod
//! sat behind the chain until it was restarted by hand (16:48, 17:01,
//! 17:59, 18:20Z), and each restart's startup heal logged
//! `heal_pointers_atomic: reconciled pointers at height N-1 (was
//! indexed=N-1, runtime_tip=N)`: the rollback had also left
//! `/__INTERNAL/tip-height` behind.
//!
//! These tests run the real RocksDB storage adapter (real `commit_atomic`
//! in-order check, real rollback), the real `SnapshotMetashrewSync` engine
//! and the production near-tip fetcher step and failure recovery from
//! `crate::near_tip`, against a mock node serving real Bitcoin headers so
//! the continuity check runs for real. Fetcher and processor are run one
//! step at a time, which is one valid interleaving of the production tasks.

use crate::near_tip::{
    near_tip_fetch_step, record_sent, recover_from_failed_block, NearTipFetch,
};
use bitcoin::block::{Header, Version};
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, TxMerkleNode};
use metashrew_sync::snapshot::SyncMode;
use metashrew_sync::snapshot_sync::SnapshotMetashrewSync;
use async_trait::async_trait;
use metashrew_sync::{
    AtomicBlockResult, MockBitcoinNode, MockRuntime, PreviewCall, RuntimeAdapter, RuntimeStats,
    StorageAdapter, SyncConfig, SyncResult, ViewCall, ViewResult,
};
use rockshrew_runtime::RocksDBStorageAdapter;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

type Engine = SnapshotMetashrewSync<MockBitcoinNode, RocksDBStorageAdapter, TestRuntime>;

/// Append-only state key the test runtime writes once per block, so a
/// rollback that leaves state behind shows up as a wrong "latest" value.
const STATE_KEY: &[u8] = b"/test/block-hash";

/// A runtime whose per-block batch has the same shape as the real
/// `__flush` batch: one append-only update (`<key>/<i>` = "height:value",
/// `<key>/length`), the per-height key manifest the fast rollback reads,
/// and the runtime tip pointer `/__INTERNAL/tip-height`. `commit_atomic`
/// appends the sync metadata to it, exactly as in production.
/// With `manifests: false` it omits the manifest, which forces the
/// full-scan rollback (databases indexed before manifests existed).
struct TestRuntime {
    db: Arc<rocksdb::DB>,
    manifests: bool,
    inner: MockRuntime,
}

#[async_trait]
impl RuntimeAdapter for TestRuntime {
    async fn process_block(&self, height: u32, block_data: &[u8]) -> SyncResult<()> {
        self.inner.process_block(height, block_data).await
    }
    async fn process_block_atomic(
        &self,
        height: u32,
        _block_data: &[u8],
        block_hash: &[u8],
    ) -> SyncResult<AtomicBlockResult> {
        let length_key = [STATE_KEY, b"/length"].concat();
        let len: u32 = self
            .db
            .get(&length_key)
            .unwrap()
            .map(|v| String::from_utf8_lossy(&v).parse().unwrap())
            .unwrap_or(0);
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(
            [STATE_KEY, b"/", len.to_string().as_bytes()].concat(),
            format!("{}:{}", height, hex::encode(block_hash)),
        );
        batch.put(&length_key, (len + 1).to_string());
        if self.manifests {
            batch.put(
                format!("{}{}", metashrew_runtime::smt::MANIFEST_PREFIX, height),
                metashrew_runtime::smt::serialize_key_manifest(&[STATE_KEY]),
            );
        }
        batch.put(metashrew_runtime::TIP_HEIGHT_KEY, height.to_le_bytes());
        Ok(AtomicBlockResult {
            state_root: vec![0; 32],
            batch_data: batch.data().to_vec(),
            height,
            block_hash: block_hash.to_vec(),
        })
    }
    async fn execute_view(&self, call: ViewCall) -> SyncResult<ViewResult> {
        self.inner.execute_view(call).await
    }
    async fn execute_preview(&self, call: PreviewCall) -> SyncResult<ViewResult> {
        self.inner.execute_preview(call).await
    }
    async fn get_state_root(&self, height: u32) -> SyncResult<Vec<u8>> {
        self.inner.get_state_root(height).await
    }
    async fn refresh_memory(&self) -> SyncResult<()> {
        self.inner.refresh_memory().await
    }
    async fn is_ready(&self) -> bool {
        self.inner.is_ready().await
    }
    async fn get_stats(&self) -> SyncResult<RuntimeStats> {
        self.inner.get_stats().await
    }
}

/// (height, display-order hash, serialized block)
type B = (u32, Vec<u8>, Vec<u8>);

fn display(h: &BlockHash) -> Vec<u8> {
    let mut v = h.to_byte_array().to_vec();
    v.reverse();
    v
}

/// Blocks `from..=to` on top of `prev`; `salt` makes competing branches differ.
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
        out.push((h, display(&prev), bitcoin::consensus::serialize(&block)));
    }
    out
}

fn hash_of(b: &B) -> BlockHash {
    let block: Block = bitcoin::consensus::deserialize(&b.2).unwrap();
    block.block_hash()
}

struct Harness {
    engine: Engine,
    storage: RocksDBStorageAdapter,
    db: Arc<rocksdb::DB>,
    node: MockBitcoinNode,
    /// The fetcher's `last_sent_height` watermark, as in `run()`.
    last_sent: AtomicI64,
    /// Canonical chain as the node currently serves it, by height.
    canonical: Vec<B>,
    _dir: tempfile::TempDir,
}

#[derive(Debug, Default)]
struct PumpStats {
    committed: u32,
    failures: u32,
    errors: Vec<String>,
}

impl Harness {
    /// Index chain 0..=tip block by block through the near-tip path.
    async fn new(tip: u32) -> Self {
        Self::with(tip, true).await
    }

    async fn with(tip: u32, manifests: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(rocksdb::DB::open(&opts, dir.path()).unwrap());
        let storage = RocksDBStorageAdapter::new(db.clone());
        let node = MockBitcoinNode::new();
        let canonical = chain(BlockHash::all_zeros(), 0, tip, 1);
        for (h, hash, data) in &canonical {
            node.add_block(*h, hash.clone(), data.clone());
        }
        let config = SyncConfig {
            start_block: 0,
            exit_at: None,
            pipeline_size: None,
            max_reorg_depth: 100,
            reorg_check_threshold: 6,
            enable_startup_heal: false,
        };
        let engine = SnapshotMetashrewSync::new(
            node.clone(),
            storage.clone(),
            TestRuntime {
                db: db.clone(),
                manifests,
                inner: MockRuntime::new(),
            },
            config,
            SyncMode::Normal,
        );
        engine.init().await;
        // Bootstrap below the near-tip window directly; the tests only
        // exercise the near-tip path.
        let near = tip.saturating_sub(5);
        for (h, hash, data) in canonical.iter().filter(|b| b.0 < near) {
            engine
                .process_block(*h, data.clone(), hash.clone())
                .await
                .unwrap();
        }
        let mut hx = Self {
            engine,
            storage,
            db,
            node,
            last_sent: AtomicI64::new(near as i64 - 1),
            canonical,
            _dir: dir,
        };
        let st = hx.pump().await;
        assert_eq!(st.failures, 0, "clean initial sync: {:?}", st.errors);
        hx.assert_synced(tip).await;
        hx
    }

    /// Replace the chain from `from` with a competing branch up to `to`
    /// (`to` may equal the current tip: a same-height reorg).
    fn reorg(&mut self, from: u32, to: u32, salt: u32) {
        let parent = hash_of(&self.canonical[(from - 1) as usize]);
        let branch = chain(parent, from, to, salt);
        self.node.simulate_reorg(from, branch.clone());
        self.canonical.truncate(from as usize);
        self.canonical.extend(branch);
    }

    /// A new block on top of the current canonical tip.
    fn extend(&mut self, salt: u32) {
        let tip = self.canonical.last().unwrap();
        let next = chain(hash_of(tip), tip.0 + 1, tip.0 + 1, salt);
        self.node.add_block(next[0].0, next[0].1.clone(), next[0].2.clone());
        self.canonical.extend(next);
    }

    /// Run fetcher -> processor -> indexer steps the way `run()` composes
    /// them until the fetcher has been idle a few times in a row (nothing
    /// left to fetch), with a hard bound so a livelock fails the test.
    async fn pump(&self) -> PumpStats {
        let mut st = PumpStats::default();
        let mut idle = 0;
        for _ in 0..200 {
            let processor_tip = self.engine.current_height();
            let last_sent_snapshot = self.last_sent.load(Ordering::SeqCst);
            let start = if last_sent_snapshot < 0 {
                processor_tip
            } else {
                std::cmp::max((last_sent_snapshot as u32).saturating_add(1), processor_tip)
            };
            let remote_tip = self.canonical.last().unwrap().0;
            assert!(
                remote_tip.saturating_sub(start) <= self.engine.config.reorg_check_threshold,
                "test only drives the near-tip path"
            );
            match near_tip_fetch_step(&self.engine, &self.last_sent, last_sent_snapshot).await {
                NearTipFetch::Send(block) => {
                    idle = 0;
                    record_sent(&self.last_sent, block.height);
                    match self
                        .engine
                        .process_block(block.height, block.block_data, block.block_hash)
                        .await
                    {
                        Ok(()) => st.committed += 1,
                        Err(e) => {
                            st.failures += 1;
                            let msg = e.to_string();
                            st.errors.push(format!("{}: {}", block.height, msg));
                            recover_from_failed_block(
                                &self.engine,
                                &self.last_sent,
                                block.height,
                                &msg,
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
                NearTipFetch::Duplicate => {}
                NearTipFetch::Idle => {
                    idle += 1;
                    if idle >= 3 {
                        return st;
                    }
                }
                NearTipFetch::Failed(e) => panic!("fetch failed: {}", e),
            }
        }
        panic!("indexer did not settle within 200 steps: {:?}", st);
    }

    fn raw_u32(&self, key: &[u8]) -> Option<u32> {
        self.db
            .get(key)
            .unwrap()
            .map(|v| u32::from_le_bytes(v[..4].try_into().unwrap()))
    }

    /// Indexed tip == `tip`, every stored block hash is the canonical one,
    /// no hash record above the tip, and both height pointers agree (what
    /// the startup heal had to reconcile in production).
    async fn assert_synced(&self, tip: u32) {
        let indexed = self.storage.get_indexed_height().await.unwrap();
        assert_eq!(
            indexed, tip,
            "indexed tip {} but the node's tip is {}: the indexer is stalled behind the chain",
            indexed, tip
        );
        for b in &self.canonical {
            assert_eq!(
                self.storage.get_block_hash(b.0).await.unwrap().as_deref(),
                Some(b.1.as_slice()),
                "stored hash at {} is not the canonical block",
                b.0
            );
        }
        assert_eq!(self.storage.get_block_hash(tip + 1).await.unwrap(), None);
        assert_eq!(self.engine.current_height(), tip + 1, "cursor = tip + 1");
        assert_eq!(
            self.raw_u32(b"/__INTERNAL/tip-height"),
            Some(tip),
            "runtime tip pointer must follow __INTERNAL/height"
        );
        // Indexed state: one append-only entry per canonical block, the
        // latest being the tip's hash (nothing left over from a dead branch).
        let len: u32 = self
            .db
            .get([STATE_KEY, b"/length"].concat())
            .unwrap()
            .map(|v| String::from_utf8_lossy(&v).parse().unwrap())
            .unwrap();
        assert_eq!(len, tip + 1, "one state entry per canonical block");
        let latest = self
            .db
            .get([STATE_KEY, b"/", tip.to_string().as_bytes()].concat())
            .unwrap()
            .unwrap();
        let tip_block = &self.canonical[tip as usize];
        assert_eq!(
            String::from_utf8(latest).unwrap(),
            format!("{}:{}", tip, hex::encode(&tip_block.1)),
            "latest indexed state is the canonical tip"
        );
    }
}

/// The testnet4 shape: a 1-block reorg at the tip that does NOT extend the
/// chain. Before the fix the fetcher idled at floor N+1 and N was never
/// re-fetched: indexed stayed at N-1 until a restart.
#[tokio::test]
async fn same_height_one_block_reorg_heals_without_restart() {
    let mut hx = Harness::new(10).await;
    hx.reorg(10, 10, 2);
    let st = hx.pump().await;
    hx.assert_synced(10).await;
    assert_eq!(st.failures, 0, "no block should fail: {:?}", st.errors);

    // And the chain keeps moving afterwards.
    hx.extend(3);
    hx.pump().await;
    hx.assert_synced(11).await;
}

/// A 1-block reorg where the winning branch is already one block longer.
#[tokio::test]
async fn one_block_reorg_with_extension_heals_without_restart() {
    let mut hx = Harness::new(10).await;
    hx.reorg(10, 11, 2);
    let st = hx.pump().await;
    hx.assert_synced(11).await;
    assert_eq!(st.failures, 0, "no block should fail: {:?}", st.errors);
}

/// Multi-block reorgs: three blocks replaced at the same height, and three
/// replaced plus two more on top.
#[tokio::test]
async fn multi_block_reorg_heals_without_restart() {
    let mut hx = Harness::new(12).await;
    hx.reorg(10, 12, 2);
    let st = hx.pump().await;
    hx.assert_synced(12).await;
    assert_eq!(st.failures, 0, "no block should fail: {:?}", st.errors);

    hx.reorg(9, 14, 3);
    let st = hx.pump().await;
    hx.assert_synced(14).await;
    assert_eq!(st.failures, 0, "no block should fail: {:?}", st.errors);
}

/// Same shapes on a database without per-height manifests (full-scan
/// rollback path).
#[tokio::test]
async fn reorgs_heal_without_restart_full_scan_rollback() {
    let mut hx = Harness::with(10, false).await;
    hx.reorg(10, 10, 2);
    hx.pump().await;
    hx.assert_synced(10).await;
    hx.reorg(8, 11, 3);
    hx.pump().await;
    hx.assert_synced(11).await;
}

/// Repeated reorgs, as on testnet4 where every lost race is one: a
/// same-height reorg after every new block, the same height flipping back
/// and forth (mempool.space flipped 156750 between two blocks), and a
/// reorg arriving while the previous one is only half processed.
#[tokio::test]
async fn repeated_reorgs_heal_without_restart() {
    let mut hx = Harness::new(10).await;
    let mut salt = 10;
    for round in 0..6 {
        let tip = hx.canonical.last().unwrap().0;
        salt += 1;
        hx.reorg(tip, tip, salt);
        hx.pump().await;
        hx.assert_synced(tip).await;
        salt += 1;
        hx.extend(salt);
        hx.pump().await;
        hx.assert_synced(tip + 1).await;
        let _ = round;
    }

    // Flip-flop at one height: A -> B -> A' (a third block) at the tip.
    let tip = hx.canonical.last().unwrap().0;
    for s in [100, 101, 102] {
        hx.reorg(tip, tip, s);
        hx.pump().await;
        hx.assert_synced(tip).await;
    }

    // Two reorgs back to back with no pump in between, then a deeper one.
    let tip = hx.canonical.last().unwrap().0;
    hx.reorg(tip, tip, 200);
    hx.reorg(tip - 1, tip, 201);
    hx.pump().await;
    hx.assert_synced(tip).await;
    hx.reorg(tip - 3, tip + 1, 202);
    hx.pump().await;
    hx.assert_synced(tip + 1).await;
}

