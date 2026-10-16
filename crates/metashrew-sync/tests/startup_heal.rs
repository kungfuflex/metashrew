//! v9.0.5-rc.6 startup-heal tests.
//!
//! These tests pin down the init-time pointer-divergence heal that runs
//! before the sync engine sets `current_height`. See the module-level
//! comment on `metashrew_sync::sync::heal_pointer_divergence_at_startup`
//! for the full design rationale.
//!
//! Background — what we're guarding against (production incident, mork1e
//! mainnet pod on v9.0.5-rc.5):
//!
//! On disk, three height pointers exist:
//!   - `__INTERNAL/height`           — owned by `commit_atomic`
//!   - `/__INTERNAL/tip-height`      — owned by the WASM-runtime side
//!   - `/__INTERNAL/height-to-hash/H` — per-height block-hash record
//!
//! On healthy rc.5 systems they're equal. They CAN diverge on a DB left
//! behind by a crash-looping pre-rc.5 build (WASM batch fsynced but
//! commit_atomic was rejected, or vice versa). The heal reads all three,
//! picks min-wins, validates against bitcoind when reachable, and issues
//! an atomic rollback.
//!
//! Invariants pinned here:
//!   1. Healthy state → no writes (idempotent).
//!   2. height-to-hash lagging sync-height → roll back to lag.
//!   3. Runtime-tip ahead of sync-tip → roll back both to the lower.
//!   4. Bitcoind disagrees at the candidate → walk back until match.
//!   5. `enable_startup_heal=false` → no heal (heal-flag respected).

use async_trait::async_trait;
use metashrew_sync::{
    heal_pointer_divergence_at_startup, BitcoinNodeAdapter, BlockInfo, ChainTip,
    StartupHealOutcome, StorageAdapter, StorageStats, SyncConfig, SyncError, SyncResult,
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, RwLock,
};
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock};

/// Three-pointer storage that mirrors the production RocksDB layout
/// for the v9.0.5-rc.6 heal-path:
///   - `indexed_height`: simulates `__INTERNAL/height`.
///   - `runtime_tip`: simulates `/__INTERNAL/tip-height`.
///   - `block_hashes`: simulates `/__INTERNAL/height-to-hash/H`.
///
/// The crucial difference from `MockStorage`: `runtime_tip` is tracked
/// SEPARATELY from `indexed_height`, so the test can set up the exact
/// divergence patterns the heal-path is meant to fix.
#[derive(Debug, Clone)]
struct ThreePointerStorage {
    indexed_height: Arc<TokioMutex<u32>>,
    runtime_tip: Arc<TokioMutex<u32>>,
    block_hashes: Arc<TokioMutex<HashMap<u32, Vec<u8>>>>,
    state_roots: Arc<TokioMutex<HashMap<u32, Vec<u8>>>>,
    available: Arc<RwLock<bool>>,
    /// Cumulative write counter. Bumped on every mutator. Lets a test
    /// assert "no writes happened during heal" without needing a
    /// physical RocksDB MANIFEST counter.
    write_counter: Arc<AtomicU32>,
    /// Cumulative rollback counter — separate from `write_counter` so we
    /// can distinguish "heal didn't write" from "heal rolled back but
    /// didn't write anything else."
    rollback_counter: Arc<AtomicU32>,
}

impl ThreePointerStorage {
    fn new() -> Self {
        Self {
            indexed_height: Arc::new(TokioMutex::new(0)),
            runtime_tip: Arc::new(TokioMutex::new(0)),
            block_hashes: Arc::new(TokioMutex::new(HashMap::new())),
            state_roots: Arc::new(TokioMutex::new(HashMap::new())),
            available: Arc::new(RwLock::new(true)),
            write_counter: Arc::new(AtomicU32::new(0)),
            rollback_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Seed a healthy contiguous chain from 0..=tip with synthetic
    /// hashes. Bumps the write counter but each test snapshots it
    /// AFTER seeding, so seed-writes don't contaminate the
    /// "writes during heal" assertion.
    async fn seed_healthy(&self, tip: u32) {
        *self.indexed_height.lock().await = tip;
        *self.runtime_tip.lock().await = tip;
        let mut hashes = self.block_hashes.lock().await;
        let mut roots = self.state_roots.lock().await;
        for h in 0..=tip {
            hashes.insert(h, synthetic_hash(h));
            roots.insert(h, synthetic_root(h));
        }
    }

    fn writes(&self) -> u32 {
        self.write_counter.load(Ordering::SeqCst)
    }

    fn rollbacks(&self) -> u32 {
        self.rollback_counter.load(Ordering::SeqCst)
    }

    fn snapshot_writes(&self) -> u32 {
        // No-op snapshot helper — readability.
        self.writes()
    }
}

fn synthetic_hash(height: u32) -> Vec<u8> {
    let mut h = vec![0u8; 32];
    let b = height.to_le_bytes();
    for i in 0..32 {
        h[i] = b[i % 4].wrapping_add(i as u8);
    }
    h
}

fn synthetic_root(height: u32) -> Vec<u8> {
    let mut r = vec![0u8; 32];
    let b = height.to_le_bytes();
    for i in 0..32 {
        r[i] = b[i % 4] ^ ((i as u8).wrapping_mul(7));
    }
    r
}

/// Distinct "garbage" hash used in the bitcoind-disagrees test to
/// simulate a corrupted-by-pre-rc.5-crash block-hash record.
fn garbage_hash(height: u32) -> Vec<u8> {
    let mut h = vec![0xffu8; 32];
    h[0..4].copy_from_slice(&height.to_le_bytes());
    h
}

#[async_trait]
impl StorageAdapter for ThreePointerStorage {
    async fn get_indexed_height(&self) -> SyncResult<u32> {
        Ok(*self.indexed_height.lock().await)
    }

    async fn set_indexed_height(&mut self, height: u32) -> SyncResult<()> {
        self.write_counter.fetch_add(1, Ordering::SeqCst);
        *self.indexed_height.lock().await = height;
        Ok(())
    }

    async fn store_block_hash(&mut self, height: u32, hash: &[u8]) -> SyncResult<()> {
        self.write_counter.fetch_add(1, Ordering::SeqCst);
        self.block_hashes.lock().await.insert(height, hash.to_vec());
        Ok(())
    }

    async fn get_block_hash(&self, height: u32) -> SyncResult<Option<Vec<u8>>> {
        Ok(self.block_hashes.lock().await.get(&height).cloned())
    }

    async fn store_state_root(&mut self, height: u32, root: &[u8]) -> SyncResult<()> {
        self.write_counter.fetch_add(1, Ordering::SeqCst);
        self.state_roots.lock().await.insert(height, root.to_vec());
        Ok(())
    }

    async fn get_state_root(&self, height: u32) -> SyncResult<Option<Vec<u8>>> {
        Ok(self.state_roots.lock().await.get(&height).cloned())
    }

    async fn rollback_to_height(&mut self, height: u32) -> SyncResult<()> {
        self.rollback_counter.fetch_add(1, Ordering::SeqCst);
        self.write_counter.fetch_add(1, Ordering::SeqCst);
        self.block_hashes.lock().await.retain(|&h, _| h <= height);
        self.state_roots.lock().await.retain(|&h, _| h <= height);
        *self.indexed_height.lock().await = height;
        Ok(())
    }

    async fn is_available(&self) -> bool {
        *self.available.read().unwrap()
    }

    async fn get_stats(&self) -> SyncResult<StorageStats> {
        Ok(StorageStats {
            total_entries: 0,
            indexed_height: *self.indexed_height.lock().await,
            storage_size_bytes: None,
        })
    }

    async fn get_runtime_tip_height(&self) -> SyncResult<u32> {
        Ok(*self.runtime_tip.lock().await)
    }

    async fn find_max_stored_block_hash_height(&self, floor: u32) -> SyncResult<u32> {
        let hashes = self.block_hashes.lock().await;
        let top = *self.indexed_height.lock().await;
        if top == 0 {
            return Ok(if hashes.contains_key(&0) { 0 } else { 0 });
        }
        let mut h = top;
        loop {
            if hashes.contains_key(&h) {
                return Ok(h);
            }
            if h <= floor {
                return Ok(0);
            }
            h = h.saturating_sub(1);
        }
    }

    async fn heal_pointers_atomic(&mut self, target_height: u32) -> SyncResult<()> {
        let current = *self.indexed_height.lock().await;
        let current_runtime = *self.runtime_tip.lock().await;
        if current == target_height && current_runtime == target_height {
            return Ok(());
        }
        if current > target_height {
            self.rollback_to_height(target_height).await?;
        } else if current < target_height {
            return Err(SyncError::Storage(format!(
                "heal_pointers_atomic refused to advance tip: current={} target={}",
                current, target_height
            )));
        }
        // Always re-affirm the runtime tip pointer.
        self.write_counter.fetch_add(1, Ordering::SeqCst);
        *self.runtime_tip.lock().await = target_height;
        Ok(())
    }
}

/// Bitcoin-node mock that returns deterministic canonical hashes from a
/// closure-driven oracle. Tests can hand it different oracles to
/// simulate "bitcoind has these hashes" patterns.
struct OracleNode {
    /// Returns the canonical hash bitcoind would return for a given height.
    /// `None` means bitcoind doesn't have that height (return Err).
    oracle: Box<dyn Fn(u32) -> Option<Vec<u8>> + Send + Sync>,
    tip: u32,
    connected: bool,
}

impl OracleNode {
    fn canonical(tip: u32) -> Self {
        Self {
            oracle: Box::new(|h| Some(synthetic_hash(h))),
            tip,
            connected: true,
        }
    }

    fn disconnected() -> Self {
        Self {
            oracle: Box::new(|_| None),
            tip: 0,
            connected: false,
        }
    }
}

#[async_trait]
impl BitcoinNodeAdapter for OracleNode {
    async fn get_tip_height(&self) -> SyncResult<u32> {
        if !self.connected {
            return Err(SyncError::BitcoinNode("not connected".into()));
        }
        Ok(self.tip)
    }
    async fn get_block_hash(&self, height: u32) -> SyncResult<Vec<u8>> {
        if !self.connected {
            return Err(SyncError::BitcoinNode("not connected".into()));
        }
        (self.oracle)(height)
            .ok_or_else(|| SyncError::BitcoinNode(format!("no block at {}", height)))
    }
    async fn get_block_data(&self, _height: u32) -> SyncResult<Vec<u8>> {
        Ok(vec![])
    }
    async fn get_block_info(&self, height: u32) -> SyncResult<BlockInfo> {
        Ok(BlockInfo {
            height,
            hash: self.get_block_hash(height).await?,
            data: vec![],
        })
    }
    async fn get_chain_tip(&self) -> SyncResult<ChainTip> {
        Ok(ChainTip {
            height: self.tip,
            hash: synthetic_hash(self.tip),
        })
    }
    async fn is_connected(&self) -> bool {
        self.connected
    }
}

fn make_config(enable_heal: bool) -> SyncConfig {
    SyncConfig {
        start_block: 0,
        exit_at: None,
        pipeline_size: None,
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: enable_heal,
    }
}

// ============================================================================
// Test 1: agreed pointers → no-op
// ============================================================================
#[tokio::test]
async fn startup_heal_three_pointers_agree_is_noop() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_718).await;
    let writes_before = storage.snapshot_writes();
    let rollbacks_before = storage.rollbacks();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::canonical(949_718));
    let config = make_config(true);

    let (h, outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal must succeed on consistent state");

    assert_eq!(h, 949_718, "heal must return the agreed tip unchanged");
    assert_eq!(
        outcome,
        StartupHealOutcome::AlreadyConsistent { tip: 949_718 },
        "consistent pointers must report AlreadyConsistent"
    );
    assert_eq!(
        storage.writes(),
        writes_before,
        "heal must not have issued any writes on consistent state (idempotency invariant)"
    );
    assert_eq!(
        storage.rollbacks(),
        rollbacks_before,
        "heal must not have rolled back on consistent state"
    );

    // Re-run heal twice more — still idempotent.
    let storage_arc2 = Arc::new(TokioRwLock::new(storage.clone()));
    let node2 = Arc::new(OracleNode::canonical(949_718));
    let (_h2, _o2) = heal_pointer_divergence_at_startup(node2, storage_arc2, &config)
        .await
        .unwrap();
    assert_eq!(
        storage.writes(),
        writes_before,
        "second heal must also be a no-op"
    );
}

// ============================================================================
// Test 2: block-hash records lag the indexed-height pointer.
//
// Setup: __INTERNAL/height = 949_718 and runtime_tip = 949_718, but the
// highest stored block-hash record is at 949_712 (records 949_713..=949_718
// are missing — the pre-rc.5 dual-write window where commit_atomic
// failed AFTER the height pointer was bumped on a separate write).
//
// Expected: heal picks min-wins = 949_712, rolls back to 949_712,
// `__INTERNAL/height` and `runtime_tip` both equal 949_712.
// ============================================================================
#[tokio::test]
async fn startup_heal_height_to_hash_lags_sync_height() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_712).await;
    // Now divergence: set indexed_height and runtime_tip ahead of the
    // last stored blockhash.
    *storage.indexed_height.lock().await = 949_718;
    *storage.runtime_tip.lock().await = 949_718;
    let writes_before_heal = storage.snapshot_writes();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::canonical(949_718));
    let config = make_config(true);

    let (h, outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal must succeed");

    assert_eq!(h, 949_712, "heal must pick min-wins = 949_712");
    match outcome {
        StartupHealOutcome::Healed { from, healed_to } => {
            assert_eq!(from, 949_718);
            assert_eq!(healed_to, 949_712);
        }
        other => panic!("expected Healed outcome, got {:?}", other),
    }
    assert_eq!(*storage.indexed_height.lock().await, 949_712);
    assert_eq!(*storage.runtime_tip.lock().await, 949_712);
    assert!(
        storage.writes() > writes_before_heal,
        "heal must have written to roll back forward state"
    );
    assert!(
        storage.rollbacks() >= 1,
        "heal must have called rollback_to_height at least once"
    );

    // Idempotency: re-running heal on the just-healed state is a no-op.
    let post_heal_writes = storage.snapshot_writes();
    let storage_arc2 = Arc::new(TokioRwLock::new(storage.clone()));
    let node2 = Arc::new(OracleNode::canonical(949_718));
    let (_h2, _o2) = heal_pointer_divergence_at_startup(node2, storage_arc2, &config)
        .await
        .unwrap();
    assert_eq!(
        storage.writes(),
        post_heal_writes,
        "second heal on already-healed state must not write"
    );
}

// ============================================================================
// Test 3: runtime-tip key ahead of sync-tip
//
// Setup: __INTERNAL/height = 949_712 (the sync-tip), but runtime_tip
// (the WASM-side pointer, written by __flush before commit_atomic was
// run) = 949_718, and block-hash records exist for 0..=949_712 only.
//
// This is the pre-rc.5 dual-write inconsistency: the runtime fsynced
// its tip but the sync-side commit_atomic never ran (rejected for
// some reason, or the process crashed between the two writes).
//
// Expected: heal picks min-wins = 949_712, writes runtime_tip down to
// 949_712.
// ============================================================================
#[tokio::test]
async fn startup_heal_runtime_tip_ahead_of_sync() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_712).await;
    *storage.runtime_tip.lock().await = 949_718;
    let writes_before_heal = storage.snapshot_writes();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::canonical(949_718));
    let config = make_config(true);

    let (h, outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal must succeed");

    assert_eq!(h, 949_712, "heal must pick min-wins = 949_712");
    match outcome {
        StartupHealOutcome::Healed { from, healed_to } => {
            assert_eq!(from, 949_718, "from = max of divergent pointers");
            assert_eq!(healed_to, 949_712, "healed_to = min-wins safe height");
        }
        other => panic!("expected Healed outcome, got {:?}", other),
    }
    assert_eq!(*storage.indexed_height.lock().await, 949_712);
    assert_eq!(
        *storage.runtime_tip.lock().await,
        949_712,
        "runtime_tip key MUST be brought back down to the min-wins safe height"
    );
    assert!(
        storage.writes() > writes_before_heal,
        "heal must have written the runtime_tip down"
    );
}

// ============================================================================
// Test 4: bitcoind disagrees → walk back until match
//
// Setup: All three local pointers agree at 949_718, but the stored
// block-hash at 949_718, 949_717, and 949_716 is GARBAGE (different
// from canonical). At 949_715 the stored hash matches canonical.
//
// Expected: heal walks back from 949_718 → 949_715 (the highest
// match), rolls back to 949_715.
// ============================================================================
#[tokio::test]
async fn startup_heal_bitcoind_disagrees_walks_back() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_718).await;
    // Corrupt the last three block-hash records.
    {
        let mut hashes = storage.block_hashes.lock().await;
        for h in [949_718u32, 949_717, 949_716] {
            hashes.insert(h, garbage_hash(h));
        }
    }
    let _writes_before_heal = storage.snapshot_writes();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::canonical(949_718));
    let config = make_config(true);

    let (h, outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal must succeed");

    assert_eq!(
        h, 949_715,
        "heal must walk back to the highest height where stored hash matches bitcoind"
    );
    // We expect a Healed outcome here even though the pointers agreed
    // numerically — the bitcoind walk-back triggered the rollback.
    match outcome {
        StartupHealOutcome::Healed { from, healed_to } => {
            assert_eq!(healed_to, 949_715);
            assert_eq!(from, 949_718);
        }
        // Acceptable: the heal could also report AlreadyConsistent if
        // it considers the post-walk-back state "consistent." The
        // critical invariant is the actual height ended up at 949_715
        // — but we should not have advanced past the divergence.
        StartupHealOutcome::AlreadyConsistent { tip } => {
            assert_eq!(
                tip, 949_715,
                "if AlreadyConsistent, tip must be at the walked-back height"
            );
        }
        StartupHealOutcome::Disabled => panic!("heal was enabled"),
    }
    assert_eq!(
        *storage.indexed_height.lock().await,
        949_715,
        "indexed_height must be rolled back to the canonical-matching height"
    );
}

// ============================================================================
// Test 5: --no-startup-heal disables the heal
//
// Setup: Same divergent state as test 2 (block-hash records lag
// sync-height), but `enable_startup_heal = false` in the config.
//
// Expected: no writes happen, heal returns `Disabled`, the indexed_height
// is unchanged from its divergent value.
// ============================================================================
#[tokio::test]
async fn startup_heal_disabled_via_flag() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_712).await;
    *storage.indexed_height.lock().await = 949_718;
    *storage.runtime_tip.lock().await = 949_718;
    let writes_before = storage.snapshot_writes();
    let rollbacks_before = storage.rollbacks();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::canonical(949_718));
    let config = make_config(false); // <-- heal disabled

    let (h, outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal-disabled path must not error");

    assert_eq!(
        h, 949_718,
        "heal-disabled must return raw __INTERNAL/height"
    );
    assert_eq!(
        outcome,
        StartupHealOutcome::Disabled,
        "heal-disabled must report Disabled outcome"
    );
    assert_eq!(
        storage.writes(),
        writes_before,
        "heal-disabled must NOT issue any writes"
    );
    assert_eq!(
        storage.rollbacks(),
        rollbacks_before,
        "heal-disabled must NOT have rolled back"
    );
    // State is unchanged — divergence is preserved for operator inspection.
    assert_eq!(*storage.indexed_height.lock().await, 949_718);
    assert_eq!(*storage.runtime_tip.lock().await, 949_718);
}

// ============================================================================
// Bonus: bitcoind unreachable → fall back to trusting local pointers
//
// Not in the explicit spec but stated as a constraint: "If bitcoind is
// unreachable at init time, fall back to trusting the pointers (don't
// block startup on a network blip)." Pin that here.
// ============================================================================
#[tokio::test]
async fn startup_heal_bitcoind_unreachable_trusts_local_pointers() {
    let storage = ThreePointerStorage::new();
    storage.seed_healthy(949_718).await;
    let writes_before = storage.snapshot_writes();

    let storage_arc = Arc::new(TokioRwLock::new(storage.clone()));
    let node = Arc::new(OracleNode::disconnected());
    let config = make_config(true);

    let (h, _outcome) = heal_pointer_divergence_at_startup(node, storage_arc, &config)
        .await
        .expect("heal must not error on bitcoind unreachable");

    assert_eq!(
        h, 949_718,
        "must return the on-disk indexed-height when bitcoind unreachable"
    );
    assert_eq!(
        storage.writes(),
        writes_before,
        "heal must not write when pointers agree and bitcoind is unreachable"
    );
}
