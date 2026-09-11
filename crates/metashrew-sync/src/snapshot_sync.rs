//! Snapshot-enabled synchronization engine
//!
//! This module provides a sync engine that can operate in multiple modes:
//! - Normal sync mode
//! - Snapshot creation mode
//! - Repository consumption mode
//! - Combined snapshot server mode

use async_trait::async_trait;
use bitcoin::hashes::Hash as _;
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::snapshot::*;
use crate::{
    BitcoinNodeAdapter, JsonRpcProvider, PreviewCall, RuntimeAdapter, StorageAdapter, SyncConfig,
    SyncEngine, SyncError, SyncResult, SyncStatus, ViewCall,
};

/// Snapshot-enabled synchronization engine
use std::collections::HashSet;
use tokio::sync::Mutex;

//... (rest of the file)

pub struct SnapshotMetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter,
    S: StorageAdapter,
    R: RuntimeAdapter,
{
    /// Opt-in chain-validation policy (`--enable-spv`).
    ///
    /// `None` keeps the legacy inline continuity check. `Some(_)`
    /// replaces it wholesale: the policy is the single place that
    /// decides whether a block may be applied, so there is no way for
    /// the two to disagree. See `metashrew-validator-core`.
    validator: Option<std::sync::Arc<dyn metashrew_validator_core::ChainValidator>>,

    node: Arc<N>,
    storage: Arc<RwLock<S>>,
    runtime: Arc<R>,
    pub config: SyncConfig,
    sync_mode: Arc<RwLock<SyncMode>>,

    // Snapshot components
    snapshot_provider: Arc<RwLock<Option<Box<dyn SnapshotProvider>>>>,
    snapshot_consumer: Arc<RwLock<Option<Box<dyn SnapshotConsumer>>>>,
    snapshot_server: Arc<RwLock<Option<Box<dyn SnapshotServer>>>>,

    // State tracking
    is_running: Arc<AtomicBool>,
    current_height: Arc<AtomicU32>,
    last_snapshot_height: Arc<AtomicU32>,
    snapshots_created: Arc<AtomicU32>,
    snapshots_applied: Arc<AtomicU32>,
    blocks_synced_normally: Arc<AtomicU32>,
    blocks_synced_from_snapshots: Arc<AtomicU32>,
    pub processing_heights: Arc<Mutex<HashSet<u32>>>,

    // Timing
    last_block_time: Arc<RwLock<Option<SystemTime>>>,
    last_snapshot_check: Arc<RwLock<Option<SystemTime>>>,
}

impl<N, S, R> SnapshotMetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    /// Create a new snapshot-enabled sync engine
    pub fn new(node: N, storage: S, runtime: R, config: SyncConfig, sync_mode: SyncMode) -> Self {
        Self {
            // Opt-in; installed later via `set_validator`.
            validator: None,
            node: Arc::new(node),
            storage: Arc::new(RwLock::new(storage)),
            runtime: Arc::new(runtime),
            config,
            sync_mode: Arc::new(RwLock::new(sync_mode)),

            snapshot_provider: Arc::new(RwLock::new(None)),
            snapshot_consumer: Arc::new(RwLock::new(None)),
            snapshot_server: Arc::new(RwLock::new(None)),

            is_running: Arc::new(AtomicBool::new(false)),
            current_height: Arc::new(AtomicU32::new(0)),
            last_snapshot_height: Arc::new(AtomicU32::new(0)),
            snapshots_created: Arc::new(AtomicU32::new(0)),
            snapshots_applied: Arc::new(AtomicU32::new(0)),
            blocks_synced_normally: Arc::new(AtomicU32::new(0)),
            blocks_synced_from_snapshots: Arc::new(AtomicU32::new(0)),
            processing_heights: Arc::new(Mutex::new(HashSet::new())),

            last_block_time: Arc::new(RwLock::new(None)),
            last_snapshot_check: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn init(&self) {
        // v9.0.5-rc.6: defensively heal divergent on-disk pointers BEFORE
        // computing start_height. Honors `config.enable_startup_heal`
        // (default true). See `crate::sync::heal_pointer_divergence_at_startup`
        // for the full reasoning. We call the shared helper from
        // `crate::sync` so both the plain and snapshot-enabled engines
        // get the same heal semantics.
        let healed_tip = match crate::sync::heal_pointer_divergence_at_startup(
            self.node.clone(),
            self.storage.clone(),
            &self.config,
        )
        .await
        {
            Ok((h, outcome)) => {
                match outcome {
                    crate::sync::StartupHealOutcome::Healed { from, healed_to } => {
                        warn!(
                            "startup-heal: applied (from={} healed_to={}); resuming sync from {}",
                            from, healed_to, healed_to + 1
                        );
                    }
                    crate::sync::StartupHealOutcome::AlreadyConsistent { tip } => {
                        info!("startup-heal: pointers consistent at tip {}", tip);
                    }
                    crate::sync::StartupHealOutcome::Disabled => {
                        info!("startup-heal: disabled by config; trusting __INTERNAL/height = {}", h);
                    }
                }
                h
            }
            Err(e) => {
                error!("startup-heal: FAILED ({}); refusing to advance with divergent state — exiting", e);
                panic!("startup-heal failed: {}", e);
            }
        };

        let storage = self.storage.read().await;
        let indexed_height = storage.get_indexed_height().await.unwrap_or(0);
        let start_height = if self.config.start_block > 0 && self.config.start_block > indexed_height {
            self.config.start_block
        } else if indexed_height > 0 {
            indexed_height + 1
        } else {
            self.config.start_block
        };

        if indexed_height != healed_tip && !(indexed_height == 0 && healed_tip == 0) {
            warn!(
                "startup-heal: post-heal indexed_height ({}) != healed_tip ({}); using indexed_height",
                indexed_height, healed_tip
            );
        }

        self.current_height.store(start_height, Ordering::SeqCst);
    }

    pub fn current_height(&self) -> u32 {
        self.current_height.load(Ordering::SeqCst)
    }

    /// Get a reference to the node adapter
    pub fn node(&self) -> &Arc<N> {
        &self.node
    }

    /// Get a reference to the storage adapter
    pub fn storage(&self) -> &Arc<RwLock<S>> {
        &self.storage
    }

    /// Get a reference to the runtime adapter
    pub fn runtime(&self) -> &Arc<R> {
        &self.runtime
    }

    pub async fn get_height(&self) -> SyncResult<u32> {
        let storage = self.storage.read().await;
        storage.get_indexed_height().await
    }

    pub async fn get_next_block_data(&self) -> SyncResult<Option<(u32, Vec<u8>, Vec<u8>)>> {
        self.get_next_block_data_with_floor(None).await
    }

    /// v9.0.5-rc.13: `min_target_height` floor for the next-block-to-fetch
    /// decision. Defends against the fetcher-wedge bug where
    /// `handle_reorg` silently lowers `self.current_height` (in non-reorg
    /// edge cases — e.g. a transient bouncer/bitcoind hash mismatch) and
    /// then `get_next_block_data` re-fetches a block the fetcher's outer
    /// loop already sent to the processor. The outer loop drops the
    /// duplicate (via `near_tip_should_drop`), the processor idles
    /// forever, and the pod wedges. Production saw j stuck at height
    /// 951049 for 8+ minutes (block 951049 committed at 01:03:28; fetcher
    /// kept getting back 951049 because current_height never advanced
    /// past it).
    ///
    /// When `min_target_height = Some(M)`, after the (still-honored)
    /// reorg check, this method clamps the in-use height to `max(
    /// effective_current_height, M)` so the fetcher never re-fetches
    /// what the outer loop has already enqueued. Reorg detection still
    /// runs and still rewinds `self.current_height` if a real reorg is
    /// found; the floor only changes which block is RETURNED here.
    pub async fn get_next_block_data_with_floor(
        &self,
        min_target_height: Option<u32>,
    ) -> SyncResult<Option<(u32, Vec<u8>, Vec<u8>)>> {
        let mut current_height = self.current_height.load(Ordering::SeqCst);

        // Get remote tip
        let remote_tip = self.node.get_tip_height().await?;
        // v10 sync-mode: inform the storage layer of the observed bitcoind
        // tip so its `commit_atomic` can gate WAL-off behavior on the
        // bitcoind/indexer gap. Read lock only — adapter uses atomic
        // interior mutability for the tip value.
        self.storage.read().await.set_bitcoind_tip(remote_tip).await;

        // Check for reorgs only when close to the tip
        if remote_tip.saturating_sub(current_height) <= self.config.reorg_check_threshold {
             match crate::sync::handle_reorg(
                current_height,
                self.node.clone(),
                self.storage.clone(),
                self.runtime.clone(),
                &self.config,
            )
            .await
            {
                Ok(new_height) => {
                    if new_height != current_height {
                        info!("Reorg handled. Resuming from height {}", new_height);
                    }
                    current_height = new_height;
                    self.current_height.store(current_height, Ordering::SeqCst);
                }
                Err(e) => {
                    error!("Error handling reorg: {}", e);
                    return Err(e); // Propagate error for retry
                }
            }
        }

        // v9.0.5-rc.13: apply the floor AFTER the reorg check so a real
        // reorg's rollback still takes effect, but a phantom rollback
        // (no-op reorg detection that happened to clamp current_height)
        // can't cause a wedge.
        let fetch_height = match min_target_height {
            Some(m) if m > current_height => m,
            _ => current_height,
        };

        // Check exit condition
        if let Some(exit_at) = self.config.exit_at {
            if fetch_height >= exit_at {
                info!("Fetcher reached exit height {}", exit_at);
                return Ok(None);
            }
        }

        // Check if we need to wait for new blocks
        if fetch_height > remote_tip {
            debug!(
                "Waiting for new blocks: fetch_height={}, tip={}",
                fetch_height, remote_tip
            );
            return Ok(None);
        }

        // Fetch block
        match self.node.get_block_info(fetch_height).await {
            Ok(block_info) => {
                info!(
                    "Fetched block {} ({} bytes)",
                    fetch_height,
                    block_info.data.len()
                );
                Ok(Some((fetch_height, block_info.data, block_info.hash)))
            }
            Err(e) => {
                error!("Failed to fetch block {}: {}", fetch_height, e);
                Err(e.into())
            }
        }
    }


    /// Install an opt-in chain-validation policy.
    ///
    /// Replaces the built-in continuity check for every subsequent
    /// block. Call before starting the sync loop.
    pub fn set_validator(
        &mut self,
        validator: std::sync::Arc<dyn metashrew_validator_core::ChainValidator>,
    ) {
        self.validator = Some(validator);
    }

    /// Validate block chain continuity like a light client (SPV-style)
    ///
    /// This performs two validations:
    /// 1. Computes the block hash from the header and verifies it matches the provided hash
    /// 2. Verifies the block's prev_blockhash matches our computed hash of the previous block
    ///
    /// This is more secure than trusting stored hashes - we verify the actual block data.
    async fn validate_block_connects(&self, height: u32, block_data: &[u8], provided_hash: &[u8]) -> SyncResult<bool> {
        use bitcoin::consensus::Encodable;
        use sha2::{Sha256, Digest};

        // Opt-in policy takes over completely when configured. The
        // inline checks below stay as the default so behaviour is
        // unchanged for anyone who has not passed --enable-spv.
        if let Some(validator) = &self.validator {
            let req = metashrew_validator_core::ValidationRequest {
                height,
                header: block_data.get(..80).unwrap_or(block_data).to_vec(),
                claimed_hash: {
                    let mut h = [0u8; 32];
                    if provided_hash.len() != 32 {
                        error!(
                            "\u{26a0} block {} was given a {}-byte hash, expected 32",
                            height,
                            provided_hash.len()
                        );
                        return Ok(false);
                    }
                    h.copy_from_slice(provided_hash);
                    h
                },
                prev_hash: if height == 0 {
                    None
                } else {
                    let storage = self.storage.read().await;
                    let stored = storage.get_block_hash(height - 1).await?;
                    drop(storage);
                    match stored {
                        Some(v) if v.len() == 32 => {
                            let mut h = [0u8; 32];
                            h.copy_from_slice(&v);
                            Some(h)
                        }
                        _ => None,
                    }
                },
            };

            let verdict = validator.validate(&req);
            if !verdict.accepted {
                error!("\u{26a0} SPV REJECT at height {}: {}", height, verdict.reason);
            }
            return Ok(verdict.accepted);
        }

        // Decode the block
        let block: bitcoin::Block = bitcoin::consensus::deserialize(block_data)
            .map_err(|e| SyncError::BlockProcessing {
                height,
                message: format!("Failed to deserialize block: {}", e),
            })?;

        // Step 1: Compute block hash from header (double SHA256)
        let mut header_bytes = Vec::with_capacity(80);
        block.header.consensus_encode(&mut header_bytes)
            .map_err(|e| SyncError::BlockProcessing {
                height,
                message: format!("Failed to encode block header: {}", e),
            })?;

        let first_hash = Sha256::digest(&header_bytes);
        let second_hash = Sha256::digest(&first_hash);
        let mut computed_hash: Vec<u8> = second_hash.to_vec();
        computed_hash.reverse(); // Convert to display order (big-endian) to match bitcoind

        // Verify computed hash matches provided hash
        if computed_hash != provided_hash {
            error!(
                "⚠ BLOCK HASH MISMATCH at height {}: Computed {} but received {}",
                height,
                hex::encode(&computed_hash),
                hex::encode(provided_hash)
            );
            return Ok(false);
        }

        debug!(
            "✓ Block {} hash verified: {}...{}",
            height,
            hex::encode(&computed_hash[..4]),
            hex::encode(&computed_hash[28..])
        );

        // Genesis block has no previous block to check
        if height == 0 {
            return Ok(true);
        }

        // Step 2: Verify prev_blockhash matches stored hash of previous block
        // Convert prev_blockhash to display order to match stored format
        let mut block_prev_hash: Vec<u8> = block.header.prev_blockhash.to_byte_array().to_vec();
        block_prev_hash.reverse();

        // Get the stored hash of the previous block
        let storage = self.storage.read().await;
        let stored_prev_hash = storage.get_block_hash(height - 1).await?;
        drop(storage);

        match stored_prev_hash {
            Some(stored_hash) => {
                if stored_hash != block_prev_hash {
                    error!(
                        "⚠ CHAIN DISCONTINUITY at height {}: Block's prev_blockhash {} does not match stored hash {} of block {}",
                        height,
                        hex::encode(&block_prev_hash),
                        hex::encode(&stored_hash),
                        height - 1
                    );
                    Ok(false)
                } else {
                    debug!(
                        "✓ Block {} connects to previous block {} (prev_hash: {}...{})",
                        height,
                        height - 1,
                        hex::encode(&block_prev_hash[..4]),
                        hex::encode(&block_prev_hash[28..])
                    );
                    Ok(true)
                }
            }
            None => {
                warn!(
                    "No stored hash for block {} - unable to validate chain continuity for block {}",
                    height - 1,
                    height
                );
                // Allow processing to continue, but log the issue
                Ok(true)
            }
        }
    }

    /// Process a single block atomically. See `MetashrewSync::process_block`
    /// for the full invariants — this is the snapshot-enabled engine's
    /// equivalent and follows the same infinite-retry-with-exponential-backoff
    /// policy: block until atomic commit succeeds, no silent fallback to a
    /// non-atomic write path, no `process::exit`.
    pub async fn process_block(
        &self,
        height: u32,
        block_data: Vec<u8>,
        block_hash: Vec<u8>,
    ) -> SyncResult<()> {
        // SPV-style continuity check.
        if !self.validate_block_connects(height, &block_data, &block_hash).await? {
            return Err(SyncError::BlockProcessing {
                height,
                message: format!(
                    "Block does not connect to previous block - possible reorg or chain inconsistency"
                ),
            });
        }

        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        loop {
            attempt = attempt.saturating_add(1);

            // v9.0.5-rc.9: staleness guard. If the engine's `current_height`
            // has advanced past `height` while we were either waiting to be
            // called or sleeping between retry attempts, the block has
            // already been applied by another code path (or by a previous
            // iteration whose commit succeeded but whose return value was
            // dropped on a panic, etc.). Treat that as a successful no-op
            // and bail. Without this guard, `commit_atomic` would reject the
            // request with the rc.5 "out-of-order commit rejected" error on
            // every iteration, and the rc.3 infinite-retry policy would
            // spin forever — exactly the production wedge documented in
            // tests/snapshot_path_stale_retry_test.rs.
            let current = self.current_height.load(Ordering::SeqCst);
            if height < current {
                debug!(
                    "snapshot-path: block {} stale relative to current_height ({}); \
                     exiting retry loop after {} attempt(s) — caller raced another \
                     path that advanced the tip past this height",
                    height, current, attempt
                );
                return Ok(());
            }

            // Max-attempts bound (companion to the staleness guard above):
            // covers the OTHER way the retry loop wedged in production,
            // where commit_atomic returns a non-staleness error every time
            // (mork1e 2026-05-21: block 892936 retried 600+ attempts on
            // "Too many open files"). See MAX_ATOMIC_COMMIT_ATTEMPTS in
            // sync.rs for the rationale on the specific cap.
            if attempt > crate::sync::MAX_ATOMIC_COMMIT_ATTEMPTS {
                let msg = last_err.unwrap_or_else(|| "no error captured".to_string());
                return Err(SyncError::Storage(format!(
                    "snapshot-path: commit_atomic at height {} exceeded \
                     max-retry budget ({} attempts): last error: {}",
                    height,
                    crate::sync::MAX_ATOMIC_COMMIT_ATTEMPTS,
                    msg
                )));
            }

            if attempt > 1 {
                let backoff_ms = crate::sync::atomic_retry_backoff_ms(attempt);
                if backoff_ms > 0 {
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }

            match self
                .runtime
                .process_block_atomic(height, &block_data, &block_hash)
                .await
            {
                Ok(result) => {
                    let commit_res = {
                        let mut storage = self.storage.write().await;
                        storage
                            .commit_atomic(height, &result.block_hash, &result.batch_data)
                            .await
                    };
                    match commit_res {
                        Ok(()) => {
                            info!(
                                "Block {} committed atomically (snapshot path, attempt {})",
                                height, attempt
                            );
                            self.current_height.store(height + 1, Ordering::SeqCst);
                            self.blocks_synced_normally.fetch_add(1, Ordering::SeqCst);
                            return Ok(());
                        }
                        Err(commit_err) => {
                            let msg = format!("{}", commit_err);

                            // rc.15: an out-of-order rejection is DETERMINISTIC —
                            // it is a function of the gap between `height` and the
                            // committed tip, and re-executing this block changes
                            // neither. Retrying costs ~43 min/block in production
                            // and delays the resync signal that is the only real
                            // fix. Mirrors the same guard in `sync.rs`; both paths
                            // need it (rc.6/rc.7 patched only one and the wedge
                            // survived on the other).
                            if msg.contains("out-of-order commit rejected") {
                                error!(
                                    "snapshot-path atomic commit: block {}: {} — not retryable; surfacing \
                                     immediately for reorg handling",
                                    height, msg
                                );
                                return Err(SyncError::Storage(format!(
                                    "commit_atomic: {}",
                                    msg
                                )));
                            }

                            crate::sync::log_atomic_retry_failure(
                                "snapshot-path atomic commit",
                                height,
                                attempt,
                                &msg,
                            );
                            last_err = Some(format!("commit_atomic: {}", msg));
                        }
                    }
                }
                Err(atomic_err) => {
                    let msg = format!("{}", atomic_err);
                    crate::sync::log_atomic_retry_failure(
                        "snapshot-path atomic block execution",
                        height,
                        attempt,
                        &msg,
                    );
                    last_err = Some(format!("process_block_atomic: {}", msg));
                }
            }

            if let Err(e) = self.runtime.refresh_memory().await {
                warn!(
                    "refresh_memory() between retries failed at height {}: {} (continuing)",
                    height, e
                );
            }
        }
    }

    /// Set the snapshot provider
    pub async fn set_snapshot_provider(&self, provider: Box<dyn SnapshotProvider>) {
        let mut sp = self.snapshot_provider.write().await;
        *sp = Some(provider);
    }

    /// Set the snapshot consumer
    pub async fn set_snapshot_consumer(&self, consumer: Box<dyn SnapshotConsumer>) {
        let mut sc = self.snapshot_consumer.write().await;
        *sc = Some(consumer);
    }

    /// Set the snapshot server
    pub async fn set_snapshot_server(&self, server: Box<dyn SnapshotServer>) {
        let mut ss = self.snapshot_server.write().await;
        *ss = Some(server);
    }

    /// Initialize the sync engine


    /// Check if we should try to use snapshots for fast sync
    async fn should_attempt_snapshot_sync(&self) -> SyncResult<bool> {
        let mode = self.sync_mode.read().await;
        match &*mode {
            SyncMode::Repo(_config) => {
                let current_height = self.current_height.load(Ordering::SeqCst);
                let tip_height = self.node.get_tip_height().await?;
                // v10 sync-mode: see WAL-off gating note above.
                self.storage.read().await.set_bitcoind_tip(tip_height).await;

                if let Some(consumer) = self.snapshot_consumer.read().await.as_ref() {
                    consumer
                        .should_use_snapshots(current_height, tip_height)
                        .await
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    /// Attempt to sync using snapshots
    async fn attempt_snapshot_sync(&self) -> SyncResult<bool> {
        let current_height = self.current_height.load(Ordering::SeqCst);
        let tip_height = self.node.get_tip_height().await?;
        // v10 sync-mode: see WAL-off gating note above.
        self.storage.read().await.set_bitcoind_tip(tip_height).await;

        if let Some(consumer) = self.snapshot_consumer.write().await.as_mut() {
            if let Some(best_snapshot) = consumer
                .get_best_snapshot(current_height, tip_height)
                .await?
            {
                info!(
                    "Applying snapshot at height {} (current: {}, tip: {})",
                    best_snapshot.height, current_height, tip_height
                );

                consumer.apply_snapshot(&best_snapshot).await?;

                // Update our state
                self.current_height
                    .store(best_snapshot.height, Ordering::SeqCst);
                self.snapshots_applied.fetch_add(1, Ordering::SeqCst);
                self.blocks_synced_from_snapshots.fetch_add(
                    best_snapshot.height.saturating_sub(current_height),
                    Ordering::SeqCst,
                );

                // Update storage
                {
                    let mut storage = self.storage.write().await;
                    storage.set_indexed_height(best_snapshot.height).await?;
                    storage
                        .store_block_hash(best_snapshot.height, &best_snapshot.block_hash)
                        .await?;
                }

                info!(
                    "Successfully applied snapshot, jumped from height {} to {}",
                    current_height, best_snapshot.height
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Process a single block with snapshot considerations
    async fn process_block_with_snapshots(
        &mut self,
        height: u32,
        block_data: Vec<u8>,
    ) -> SyncResult<()> {
        // Atomic path: same infinite-retry-with-exponential-backoff policy as
        // `process_block`. The block hash comes from the node here because
        // this entry point doesn't receive it from a caller; it's the
        // snapshot-sync-loop's own fetcher boundary. The SPV continuity check
        // still happens inside the runtime's `validate_block_connects` path
        // when callers route through `process_block`, but this convenience
        // method on the snapshot loop skips it because the loop has already
        // done reorg handling and chain-continuity inspection upstream.
        let block_hash = self.node.get_block_hash(height).await?;

        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        loop {
            attempt = attempt.saturating_add(1);

            // v9.0.5-rc.9: staleness guard — see the matching block in
            // `process_block` for the full rationale. The
            // `snapshot-loop`-labelled commit-rejection retries were the
            // other path that could wedge with the same "infinite-retry
            // against an already-advanced tip" failure mode.
            let current = self.current_height.load(Ordering::SeqCst);
            if height < current {
                debug!(
                    "snapshot-loop: block {} stale relative to current_height ({}); \
                     exiting retry loop after {} attempt(s) — caller raced another \
                     path that advanced the tip past this height",
                    height, current, attempt
                );
                return Ok(());
            }

            // Max-attempts bound: see MAX_ATOMIC_COMMIT_ATTEMPTS in sync.rs
            // for the rationale (mork1e's 600-attempt wedge on block
            // 892936 with persistent "Too many open files"). At this
            // cap the loop has been retrying for ~10-15 minutes against
            // a persistent failure — operator needs the signal.
            if attempt > crate::sync::MAX_ATOMIC_COMMIT_ATTEMPTS {
                let msg = last_err.unwrap_or_else(|| "no error captured".to_string());
                return Err(SyncError::Storage(format!(
                    "snapshot-loop: commit_atomic at height {} exceeded \
                     max-retry budget ({} attempts): last error: {}",
                    height,
                    crate::sync::MAX_ATOMIC_COMMIT_ATTEMPTS,
                    msg
                )));
            }

            if attempt > 1 {
                let backoff_ms = crate::sync::atomic_retry_backoff_ms(attempt);
                if backoff_ms > 0 {
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }

            match self
                .runtime
                .process_block_atomic(height, &block_data, &block_hash)
                .await
            {
                Ok(result) => {
                    let commit_res = {
                        let mut storage = self.storage.write().await;
                        storage
                            .commit_atomic(height, &result.block_hash, &result.batch_data)
                            .await
                    };
                    match commit_res {
                        Ok(()) => {
                            info!(
                                "Block {} committed atomically (snapshot loop, attempt {})",
                                height, attempt
                            );
                            self.current_height.store(height + 1, Ordering::SeqCst);
                            self.blocks_synced_normally.fetch_add(1, Ordering::SeqCst);
                            {
                                let mut last_time = self.last_block_time.write().await;
                                *last_time = Some(SystemTime::now());
                            }
                            // Snapshot creation hooks (best-effort, non-atomic).
                            if let Err(e) = self.create_snapshot_if_needed(height).await {
                                warn!("Failed to create snapshot at height {}: {}", height, e);
                            }
                            let mode = self.sync_mode.read().await;
                            if matches!(*mode, SyncMode::SnapshotServer(_)) {
                                if let Some(provider) =
                                    self.snapshot_provider.read().await.as_ref()
                                {
                                    if provider.should_create_snapshot(height) {
                                        if let Some(_server) =
                                            self.snapshot_server.write().await.as_mut()
                                        {
                                            debug!(
                                                "Would register snapshot at height {} with server",
                                                height
                                            );
                                        }
                                    }
                                }
                            }
                            return Ok(());
                        }
                        Err(commit_err) => {
                            let msg = format!("{}", commit_err);

                            // rc.15: an out-of-order rejection is DETERMINISTIC —
                            // it is a function of the gap between `height` and the
                            // committed tip, and re-executing this block changes
                            // neither. Retrying costs ~43 min/block in production
                            // and delays the resync signal that is the only real
                            // fix. Mirrors the same guard in `sync.rs`; both paths
                            // need it (rc.6/rc.7 patched only one and the wedge
                            // survived on the other).
                            if msg.contains("out-of-order commit rejected") {
                                error!(
                                    "snapshot-loop atomic commit: block {}: {} — not retryable; surfacing \
                                     immediately for reorg handling",
                                    height, msg
                                );
                                return Err(SyncError::Storage(format!(
                                    "commit_atomic: {}",
                                    msg
                                )));
                            }

                            crate::sync::log_atomic_retry_failure(
                                "snapshot-loop atomic commit",
                                height,
                                attempt,
                                &msg,
                            );
                            last_err = Some(format!("commit_atomic: {}", msg));
                        }
                    }
                }
                Err(atomic_err) => {
                    let msg = format!("{}", atomic_err);
                    crate::sync::log_atomic_retry_failure(
                        "snapshot-loop atomic execution",
                        height,
                        attempt,
                        &msg,
                    );
                    last_err = Some(format!("process_block_atomic: {}", msg));
                }
            }

            if let Err(e) = self.runtime.refresh_memory().await {
                warn!(
                    "refresh_memory() between retries failed at height {}: {} (continuing)",
                    height, e
                );
            }
        }
    }

    /// Run the main sync loop with snapshot support
    async fn run_snapshot_sync_loop(&mut self) -> SyncResult<()> {
        let mut height = self.current_height.load(Ordering::SeqCst);

        // Check if we should start with snapshot sync
        if self.should_attempt_snapshot_sync().await? {
            if self.attempt_snapshot_sync().await? {
                height = self.current_height.load(Ordering::SeqCst);
                info!("Fast-forwarded to height {} using snapshots", height);
            }
        }

        // Main sync loop
        while self.is_running.load(Ordering::SeqCst) {
            // Get remote tip first
            let remote_tip = match self.node.get_tip_height().await {
                Ok(tip) => tip,
                Err(e) => {
                    error!("Failed to get tip height: {}", e);
                    sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            // v10 sync-mode: inform the storage layer of the observed
            // bitcoind tip so its `commit_atomic` can gate WAL-off behavior
            // on the bitcoind/indexer gap. Read lock only — adapter uses
            // atomic interior mutability for the tip value.
            self.storage.read().await.set_bitcoind_tip(remote_tip).await;

            // Check for reorgs only when close to the tip
            if height > 0 && remote_tip.saturating_sub(height) <= self.config.reorg_check_threshold {
                match crate::sync::handle_reorg(
                    height,
                    self.node.clone(),
                    self.storage.clone(),
                    self.runtime.clone(),
                    &self.config,
                )
                .await
                {
                    Ok(reorg_height) => {
                        if reorg_height < height {
                            height = reorg_height;
                            info!("Reorg handled. Resuming from height {}", height);
                            continue;
                        }
                    }
                    Err(e) => {
                        error!("Error handling reorg: {}", e);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                }
            }

            // Check exit condition
            if let Some(exit_at) = self.config.exit_at {
                if height >= exit_at {
                    info!("Reached exit height {}", exit_at);
                    break;
                }
            }

            // Check if we need to wait for new blocks
            if height > remote_tip {
                debug!(
                    "Waiting for new blocks: current={}, tip={}",
                    height, remote_tip
                );

                // In repo mode, periodically check for new snapshots
                if matches!(*self.sync_mode.read().await, SyncMode::Repo(_)) {
                    let should_check = {
                        let last_check = self.last_snapshot_check.read().await;
                        match *last_check {
                            Some(last) => {
                                SystemTime::now()
                                    .duration_since(last)
                                    .unwrap_or_default()
                                    .as_secs()
                                    > 300
                            } // Check every 5 minutes
                            None => true,
                        }
                    };

                    if should_check {
                        if self.should_attempt_snapshot_sync().await? {
                            if self.attempt_snapshot_sync().await? {
                                height = self.current_height.load(Ordering::SeqCst);
                                continue;
                            }
                        }

                        let mut last_check = self.last_snapshot_check.write().await;
                        *last_check = Some(SystemTime::now());
                    }
                }

                sleep(Duration::from_secs(3)).await;
                continue;
            }

            // Fetch and process block
            match self.node.get_block_data(height).await {
                Ok(block_data) => {
                    info!("Processing block {} ({} bytes)", height, block_data.len());

                    if let Err(e) = self.process_block_with_snapshots(height, block_data).await {
                        error!("Failed to process block {}: {}", height, e);
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    height += 1;
                }
                Err(e) => {
                    error!("Failed to fetch block {}: {}", height, e);
                    sleep(Duration::from_secs(1)).await;
                    // CRITICAL FIX: Don't advance height on fetch failure
                    // Continue the loop to retry the same block
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<N, S, R> SnapshotSyncEngine for SnapshotMetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    fn get_sync_mode(&self) -> &SyncMode {
        // This is a bit tricky since we need to return a reference
        // In practice, this would need to be redesigned or use a different approach
        unimplemented!("Use async version get_sync_mode_async")
    }

    async fn set_sync_mode(&mut self, mode: SyncMode) -> SyncResult<()> {
        let mut sync_mode = self.sync_mode.write().await;
        *sync_mode = mode;
        Ok(())
    }

    async fn process_block_with_snapshots(
        &mut self,
        height: u32,
        block_data: &[u8],
    ) -> SyncResult<()> {
        self.process_block_with_snapshots(height, block_data.to_vec())
            .await
    }

    async fn check_and_apply_snapshots(&mut self) -> SyncResult<bool> {
        if self.should_attempt_snapshot_sync().await? {
            self.attempt_snapshot_sync().await
        } else {
            Ok(false)
        }
    }

    async fn create_snapshot_if_needed(&mut self, height: u32) -> SyncResult<bool> {
        if let Some(provider) = self.snapshot_provider.write().await.as_mut() {
            if provider.should_create_snapshot(height) {
                info!("Creating snapshot at height {}", height);

                match provider.create_snapshot(height).await {
                    Ok(metadata) => {
                        self.last_snapshot_height.store(height, Ordering::SeqCst);
                        self.snapshots_created.fetch_add(1, Ordering::SeqCst);
                        info!(
                            "Created snapshot at height {} (size: {} bytes)",
                            height, metadata.size_bytes
                        );

                        // Cleanup old snapshots
                        if let Ok(deleted) = provider.cleanup_snapshots().await {
                            if deleted > 0 {
                                debug!("Cleaned up {} old snapshots", deleted);
                            }
                        }

                        return Ok(true);
                    }
                    Err(e) => {
                        error!("Failed to create snapshot at height {}: {}", height, e);
                        return Err(e);
                    }
                }
            }
        }

        Ok(false)
    }

    async fn get_snapshot_stats(&self) -> SyncResult<SnapshotSyncStats> {
        let current_height = self.current_height.load(Ordering::SeqCst);
        let tip_height = self.node.get_tip_height().await?;
        // v10 sync-mode: see WAL-off gating note above.
        self.storage.read().await.set_bitcoind_tip(tip_height).await;
        let sync_mode = format!("{:?}", *self.sync_mode.read().await);

        Ok(SnapshotSyncStats {
            current_height,
            tip_height,
            sync_mode,
            snapshots_created: self.snapshots_created.load(Ordering::SeqCst),
            snapshots_applied: self.snapshots_applied.load(Ordering::SeqCst),
            last_snapshot_height: {
                let height = self.last_snapshot_height.load(Ordering::SeqCst);
                if height > 0 {
                    Some(height)
                } else {
                    None
                }
            },
            blocks_synced_normally: self.blocks_synced_normally.load(Ordering::SeqCst),
            blocks_synced_from_snapshots: self.blocks_synced_from_snapshots.load(Ordering::SeqCst),
        })
    }

    async fn process_next_block(&mut self) -> SyncResult<Option<u32>> {
        let mut height = self.current_height.load(Ordering::SeqCst);

        if height == 0 {
            self.init().await;
            height = self.current_height.load(Ordering::SeqCst);
        }

        if height > 0 {
            match crate::sync::handle_reorg(
                height,
                self.node.clone(),
                self.storage.clone(),
                self.runtime.clone(),
                &self.config,
            )
            .await
            {
                Ok(reorg_height) => {
                    if reorg_height < height {
                        self.current_height.store(reorg_height, Ordering::SeqCst);
                        return Ok(Some(reorg_height));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        if let Some(exit_at) = self.config.exit_at {
            if height > exit_at {
                return Ok(None);
            }
        }

        let remote_tip = self.node.get_tip_height().await?;
        // v10 sync-mode: see WAL-off gating note above.
        self.storage.read().await.set_bitcoind_tip(remote_tip).await;
        if height > remote_tip {
            return Ok(None);
        }

        let block_data = self.node.get_block_data(height).await?;
        self.process_block_with_snapshots(height, block_data).await?;
        Ok(Some(height + 1))
    }
}

#[async_trait]
impl<N, S, R> SyncEngine for SnapshotMetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    async fn start(&mut self) -> SyncResult<()> {
        if self.is_running.load(Ordering::SeqCst) {
            return Err(SyncError::Config(
                "Sync engine is already running".to_string(),
            ));
        }

        info!("Starting snapshot-enabled Metashrew sync engine");
        self.is_running.store(true, Ordering::SeqCst);

        if !self.node.is_connected().await {
            return Err(SyncError::BitcoinNode("Node is not connected".to_string()));
        }

        let storage = self.storage.read().await;
        if !storage.is_available().await {
            return Err(SyncError::Storage("Storage is not available".to_string()));
        }
        drop(storage);

        if !self.runtime.is_ready().await {
            return Err(SyncError::Runtime("Runtime is not ready".to_string()));
        }

        self.run_snapshot_sync_loop().await?;

        Ok(())
    }

    async fn stop(&mut self) -> SyncResult<()> {
        info!("Stopping snapshot-enabled Metashrew sync engine");
        self.is_running.store(false, Ordering::SeqCst);

        // Stop snapshot server if running
        if let Some(server) = self.snapshot_server.write().await.as_mut() {
            server.stop().await?;
        }

        Ok(())
    }

    async fn get_status(&self) -> SyncResult<SyncStatus> {
        let current_height = self.current_height.load(Ordering::SeqCst);
        let tip_height = self.node.get_tip_height().await?;
        // v10 sync-mode: see WAL-off gating note above.
        self.storage.read().await.set_bitcoind_tip(tip_height).await;
        let blocks_behind = tip_height.saturating_sub(current_height);
        let last_block_time = *self.last_block_time.read().await;

        // Calculate blocks per second
        let blocks_processed = self.blocks_synced_normally.load(Ordering::SeqCst)
            + self.blocks_synced_from_snapshots.load(Ordering::SeqCst);

        let blocks_per_second = if let Some(last_time) = last_block_time {
            if let Ok(duration) = last_time.elapsed() {
                blocks_processed as f64 / duration.as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        };

        Ok(SyncStatus {
            is_running: self.is_running.load(Ordering::SeqCst),
            current_height,
            tip_height,
            blocks_behind,
            last_block_time,
            blocks_per_second,
        })
    }

    async fn process_single_block(&mut self, height: u32) -> SyncResult<()> {
        let block_data = self.node.get_block_data(height).await?;
        self.process_block_with_snapshots(height, block_data).await
    }

}

pub fn parse_height_string(height_str: &str) -> SyncResult<u32> {
    let height_part = height_str.split(':').next().unwrap_or(height_str);
    height_part
        .parse::<u32>()
        .map_err(|e| SyncError::Serialization(format!("Invalid height: {}", e)))
}

#[async_trait]
impl<N, S, R> JsonRpcProvider for SnapshotMetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    async fn metashrew_view(
        &self,
        function_name: String,
        input_hex: String,
        height: String,
    ) -> SyncResult<String> {
        let input_data = hex::decode(input_hex.trim_start_matches("0x"))
            .map_err(|e| SyncError::Serialization(format!("Invalid hex input: {}", e)))?;

        let height = if height == "latest" {
            self.current_height.load(Ordering::SeqCst).saturating_sub(1)
        } else {
            parse_height_string(&height)?
        };

        let call = ViewCall {
            function_name,
            input_data,
            height,
        };

        let result = self.runtime.execute_view(call).await?;

        Ok(format!("0x{}", hex::encode(result.data)))
    }

    async fn metashrew_preview(
        &self,
        block_hex: String,
        function_name: String,
        input_hex: String,
        height: String,
    ) -> SyncResult<String> {
        let block_data = hex::decode(block_hex.trim_start_matches("0x"))
            .map_err(|e| SyncError::Serialization(format!("Invalid hex block data: {}", e)))?;

        let input_data = hex::decode(input_hex.trim_start_matches("0x"))
            .map_err(|e| SyncError::Serialization(format!("Invalid hex input: {}", e)))?;

        let height = if height == "latest" {
            self.current_height.load(Ordering::SeqCst).saturating_sub(1)
        } else {
            parse_height_string(&height)?
        };

        let call = PreviewCall {
            block_data,
            function_name,
            input_data,
            height,
        };

        let result = self.runtime.execute_preview(call).await?;

        Ok(format!("0x{}", hex::encode(result.data)))
    }

    async fn metashrew_height(&self) -> SyncResult<u32> {
        // Use storage adapter to get the actual indexed height from database
        // This ensures consistency with the database state rather than sync engine's internal tracking
        let storage = self.storage.read().await;
        storage.get_indexed_height().await
    }

    async fn metashrew_getblockhash(&self, height: u32) -> SyncResult<String> {
        let storage = self.storage.read().await;
        match storage.get_block_hash(height).await? {
            Some(hash) => Ok(format!("0x{}", hex::encode(hash))),
            None => Err(SyncError::Storage(format!(
                "Block hash not found for height {}",
                height
            ))),
        }
    }

    async fn metashrew_snapshot(&self) -> SyncResult<serde_json::Value> {
        let storage = self.storage.read().await;
        let stats = storage.get_stats().await?;
        let snapshot_stats = self.get_snapshot_stats().await?;

        Ok(serde_json::json!({
            "enabled": true,
            "current_height": self.current_height.load(Ordering::SeqCst),
            "indexed_height": stats.indexed_height,
            "total_entries": stats.total_entries,
            "storage_size_bytes": stats.storage_size_bytes,
            "sync_mode": snapshot_stats.sync_mode,
            "snapshots_created": snapshot_stats.snapshots_created,
            "snapshots_applied": snapshot_stats.snapshots_applied,
            "last_snapshot_height": snapshot_stats.last_snapshot_height,
            "blocks_synced_normally": snapshot_stats.blocks_synced_normally,
            "blocks_synced_from_snapshots": snapshot_stats.blocks_synced_from_snapshots
        }))
    }
}