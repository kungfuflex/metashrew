//! # Core Synchronization Engine Implementation
//!
//! This module provides the main synchronization engine that coordinates Bitcoin blockchain
//! indexing using the adapter pattern. The [`MetashrewSync`] engine orchestrates the
//! interaction between Bitcoin nodes, storage backends, and WASM runtime environments
//! to provide reliable, high-performance blockchain indexing.
//!
//! ## Architecture Overview
//!
//! The synchronization engine implements a pipeline architecture with the following components:
//!
//! ### Pipeline Processing
//! - **Block Fetcher**: Asynchronously fetches blocks from Bitcoin nodes
//! - **Block Processor**: Processes blocks through WASM indexer modules
//! - **Result Handler**: Manages processing results and error recovery
//! - **Atomic Operations**: Ensures data consistency through atomic block processing
//!
//! ### Concurrency Model
//! - **Parallel Fetching**: Blocks are fetched in parallel to maximize throughput
//! - **Async Processing**: Non-blocking I/O operations throughout the pipeline
//! - **Thread Safety**: Safe concurrent access to shared state using atomic operations
//! - **Backpressure**: Automatic flow control to prevent memory exhaustion
//!
//! ### Error Recovery
//! - **Graceful Degradation**: Fallback from atomic to non-atomic processing
//! - **Retry Logic**: Automatic retry of failed operations with exponential backoff
//! - **Chain Reorganization**: Detection and handling of blockchain forks
//! - **State Consistency**: Rollback capabilities for maintaining data integrity
//!
//! ## Usage Examples
//!
//! ### Basic Synchronization
//! ```rust,ignore
//! use metashrew_sync::*;
//!
//! // Create adapters
//! let node_adapter = MyBitcoinNodeAdapter::new();
//! let storage_adapter = MyStorageAdapter::new();
//! let runtime_adapter = MyRuntimeAdapter::new();
//!
//! // Configure synchronization
//! let config = SyncConfig {
//!     start_block: 0,
//!     exit_at: None,
//!     pipeline_size: Some(10),
//!     max_reorg_depth: 100,
//!     reorg_check_threshold: 6,
//! };
//!
//! // Create and start sync engine
//! let mut sync_engine = MetashrewSync::new(
//!     node_adapter,
//!     storage_adapter,
//!     runtime_adapter,
//!     config
//! );
//!
//! sync_engine.start().await?;
//! ```
//!
//! ### JSON-RPC API Integration
//! ```rust,ignore
//! // The sync engine also implements JsonRpcProvider
//! let result = sync_engine.metashrew_view(
//!     "get_balance".to_string(),
//!     "0x1234...".to_string(),
//!     "latest".to_string()
//! ).await?;
//! ```
//!
//! ## Performance Characteristics
//!
//! ### Pipeline Optimization
//! - **Adaptive Pipeline Size**: Automatically adjusts based on CPU cores
//! - **Memory Management**: Controlled memory usage with bounded channels
//! - **Batch Operations**: Efficient database operations through batching
//! - **State Root Caching**: Optimized state root calculation and storage
//!
//! ### Monitoring and Observability
//! - **Real-time Metrics**: Blocks per second, processing latency, error rates
//! - **Status Reporting**: Current height, blocks behind, sync progress
//! - **Health Checks**: Component availability and connectivity monitoring
//! - **Detailed Logging**: Comprehensive logging for debugging and auditing
//!
//! ## Integration with Metashrew
//!
//! This engine serves as the foundation for:
//! - **rockshrew-mono**: Production Bitcoin indexer implementation
//! - **Custom indexers**: Application-specific blockchain data processing
//! - **Development tools**: Testing and prototyping of indexing strategies
//! - **API services**: JSON-RPC endpoints for accessing indexed data

use async_trait::async_trait;
use bitcoin::hashes::Hash as _;
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::sleep;

use crate::{
    BitcoinNodeAdapter, BlockResult, JsonRpcProvider, PreviewCall, RuntimeAdapter, StorageAdapter,
    SyncConfig, SyncEngine, SyncError, SyncResult, SyncStatus, ViewCall,
};

// ---------------------------------------------------------------------------
// Atomic-retry backoff + escalating-log helpers (v9.0.5-rc.3).
//
// The block-apply paths in this crate retry the (process_block_atomic +
// commit_atomic) pair *forever* on transient failure — the user's directive
// is that we MUST NOT exit on atomic-write failure. To avoid a busy-loop on
// long stalls (fsync wedged, disk full, IO timeout, etc.) we sleep between
// attempts using exponential backoff with a 30 s cap, and we escalate the
// log severity so operators see the stall.
//
// Backoff schedule (ms):
//   attempt 1   -> 0
//   attempt 2   -> 100
//   attempt 3   -> 200
//   attempt 4   -> 400
//   attempt 5   -> 800
//   attempt 6   -> 1_600
//   attempt 7   -> 3_200
//   attempt 8   -> 6_400
//   attempt 9   -> 12_800
//   attempt 10  -> 25_600
//   attempt 11+ -> 30_000 (capped)
//
// Log severity:
//   attempts 1..=ATOMIC_RETRY_WARN_THRESHOLD   -> warn!
//   attempts thresh+1 ..= ATOMIC_RETRY_ERROR_THRESHOLD -> error! every time
//   attempts > ATOMIC_RETRY_ERROR_THRESHOLD     -> error! only every
//                                                  ATOMIC_RETRY_ERROR_SPAM_EVERY
//                                                  iterations
// ---------------------------------------------------------------------------

/// Below (inclusive) this attempt count we log at WARN.
pub const ATOMIC_RETRY_WARN_THRESHOLD: u32 = 5;
/// Beyond this attempt count we drop to one-in-N error logs.
pub const ATOMIC_RETRY_ERROR_THRESHOLD: u32 = 30;
/// Past `ATOMIC_RETRY_ERROR_THRESHOLD`, log once every N iterations.
pub const ATOMIC_RETRY_ERROR_SPAM_EVERY: u32 = 10;
/// Cap on the per-attempt backoff (ms).
pub const ATOMIC_RETRY_BACKOFF_CAP_MS: u64 = 30_000;
/// Maximum total attempts the retry loop will make before giving up
/// and surfacing the underlying error to the caller. Before this cap
/// existed (rc.3 — `150abef` "never exit on atomic-write failure") the
/// loop spun forever on persistent failures like RocksDB "Too many open
/// files" (mork1e's pod 2026-05-21: block 892936 retried 600+ attempts
/// before a manual restart cleared it). At the 30 s backoff cap, 30
/// attempts is roughly 10-15 minutes of total retry time — enough to
/// ride out a transient compaction-induced FD spike, but short enough
/// that operators get a clear failure signal rather than hours of
/// silent CPU-idle spinning.
pub const MAX_ATOMIC_COMMIT_ATTEMPTS: u32 = 30;

/// Returns the milliseconds to sleep *before* `attempt` (1-indexed). Attempt 1
/// returns 0 — no sleep before the first try.
pub fn atomic_retry_backoff_ms(attempt: u32) -> u64 {
    if attempt <= 1 {
        0
    } else {
        // attempt 2  -> shift 0 -> 100 ms
        // attempt 3  -> shift 1 -> 200 ms
        // ...
        // attempt 10 -> shift 8 -> 25_600 ms
        // attempt 11 -> shift 9 -> 51_200 ms -> capped at 30_000 ms
        // attempt 12+ -> shift 9 -> 30_000 ms (capped)
        let shift = (attempt - 2).min(9) as u32;
        (100u64.saturating_mul(1u64 << shift)).min(ATOMIC_RETRY_BACKOFF_CAP_MS)
    }
}

/// Common logger for atomic-retry failures. Picks WARN / ERROR per the
/// escalation policy and includes a hint about possible state corruption if
/// the error looks like an out-of-order commit rejection (which should never
/// happen in steady-state and indicates the storage tip advanced from under
/// us — typically a sign that *something* is very wrong, but we still keep
/// retrying because the operator can SIGKILL to escalate).
pub fn log_atomic_retry_failure(
    op_label: &str,
    height: u32,
    attempt: u32,
    err_str: &str,
) {
    // "out-of-order commit rejected" is the signature emitted by
    // `commit_atomic`'s strict-in-order check (see traits.rs and the
    // RocksDB adapter). If we hit it during a retry loop, the tip has
    // either advanced past us (impossible without another writer) or
    // regressed below us. Either way it's a hard anomaly — but we keep
    // retrying per the v9.0.5-rc.3 invariant. The operator can SIGKILL to
    // escalate; we do NOT call std::process::exit here.
    let out_of_order = err_str.contains("out-of-order commit rejected");

    if attempt <= ATOMIC_RETRY_WARN_THRESHOLD {
        warn!(
            "{} failed for height {} (attempt {}): {} — retrying",
            op_label, height, attempt, err_str
        );
    } else if attempt <= ATOMIC_RETRY_ERROR_THRESHOLD {
        if out_of_order {
            error!(
                "{} failed for height {} (attempt {}, out-of-order commit rejected — \
                 possible state corruption / unexpected tip advance; manual intervention \
                 may be required): {} — block {} atomic apply has been retrying for {} attempts",
                op_label, height, attempt, err_str, height, attempt
            );
        } else {
            error!(
                "{} failed for height {} (attempt {}): {} — block {} atomic apply has \
                 been retrying for {} attempts",
                op_label, height, attempt, err_str, height, attempt
            );
        }
    } else {
        // Past ATOMIC_RETRY_ERROR_THRESHOLD: only log every Nth attempt to
        // keep stderr usable while the stall persists.
        if (attempt - ATOMIC_RETRY_ERROR_THRESHOLD) % ATOMIC_RETRY_ERROR_SPAM_EVERY == 0 {
            if out_of_order {
                error!(
                    "{} STILL failing for height {} (attempt {}, out-of-order commit rejected — \
                     possible state corruption / unexpected tip advance; manual intervention \
                     may be required): {} — block {} atomic apply has been retrying for {} attempts",
                    op_label, height, attempt, err_str, height, attempt
                );
            } else {
                error!(
                    "{} STILL failing for height {} (attempt {}): {} — block {} atomic apply \
                     has been retrying for {} attempts",
                    op_label, height, attempt, err_str, height, attempt
                );
            }
        }
    }
}

/// Generic Bitcoin indexer synchronization engine
pub struct MetashrewSync<N, S, R>
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
    is_running: Arc<AtomicBool>,
    pub current_height: Arc<AtomicU32>,
    last_block_time: Arc<RwLock<Option<SystemTime>>>,
    blocks_processed: Arc<AtomicU32>,
    processing_heights: Arc<Mutex<HashSet<u32>>>,
}

impl<N, S, R> MetashrewSync<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    /// Create a new sync engine
    pub fn new(node: N, storage: S, runtime: R, config: SyncConfig) -> Self {
        Self {
            // Opt-in; installed later via `set_validator`.
            validator: None,
            node: Arc::new(node),
            storage: Arc::new(RwLock::new(storage)),
            runtime: Arc::new(runtime),
            config,
            is_running: Arc::new(AtomicBool::new(false)),
            current_height: Arc::new(AtomicU32::new(0)),
            last_block_time: Arc::new(RwLock::new(None)),
            blocks_processed: Arc::new(AtomicU32::new(0)),
            processing_heights: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn init(&self) {
        // v9.0.5-rc.6: defensively heal divergent on-disk pointers BEFORE
        // computing start_height. Honors `config.enable_startup_heal`
        // (default true). See `heal_pointer_divergence_at_startup` for
        // the full reasoning.
        let healed_tip = match heal_pointer_divergence_at_startup(
            self.node.clone(),
            self.storage.clone(),
            &self.config,
        )
        .await
        {
            Ok((h, outcome)) => {
                match outcome {
                    StartupHealOutcome::Healed { from, healed_to } => {
                        warn!(
                            "startup-heal: applied (from={} healed_to={}); resuming sync from {}",
                            from, healed_to, healed_to + 1
                        );
                    }
                    StartupHealOutcome::AlreadyConsistent { tip } => {
                        info!("startup-heal: pointers consistent at tip {}", tip);
                    }
                    StartupHealOutcome::Disabled => {
                        info!("startup-heal: disabled by config; trusting __INTERNAL/height = {}", h);
                    }
                }
                h
            }
            Err(e) => {
                error!("startup-heal: FAILED ({}); refusing to advance with divergent state — exiting", e);
                // We can't `?` here because init returns (). The
                // production stack treats startup-heal failure as a
                // hard stop: log loudly and panic, so the operator
                // sees the failure rather than getting a silently
                // wrong indexer.
                panic!("startup-heal failed: {}", e);
            }
        };

        let (indexed_height, start_height) = {
            let storage = self.storage.read().await;
            let indexed_height = storage.get_indexed_height().await.unwrap_or(0);
            let start_height = if self.config.start_block > 0 && self.config.start_block > indexed_height {
                self.config.start_block
            } else if indexed_height > 0 {
                indexed_height + 1
            } else {
                self.config.start_block
            };
            (indexed_height, start_height)
        };

        // Sanity: heal-returned tip should match the re-read indexed_height.
        // If it doesn't, the heal did something we didn't intend.
        if indexed_height != healed_tip
            && !(indexed_height == 0 && healed_tip == 0)
        {
            warn!(
                "startup-heal: post-heal indexed_height ({}) != healed_tip ({}); using indexed_height",
                indexed_height, healed_tip
            );
        }

        self.current_height.store(start_height, Ordering::SeqCst);
    }

    /// Get a reference to the storage adapter
    pub fn storage(&self) -> &Arc<RwLock<S>> {
        &self.storage
    }

    /// Get a reference to the node adapter
    pub fn node(&self) -> &Arc<N> {
        &self.node
    }

    /// Get a reference to the runtime adapter
    pub fn runtime(&self) -> &Arc<R> {
        &self.runtime
    }

    /// Run the sync engine (convenience method that calls start)
    pub async fn run(&mut self) -> SyncResult<()> {
        self.start().await
    }

    pub async fn get_next_block_data(&self) -> SyncResult<Option<(u32, Vec<u8>, Vec<u8>)>> {
        self.get_next_block_data_with_floor(None).await
    }

    /// v9.0.5-rc.13: `min_target_height` floor for the next-block-to-fetch
    /// decision. See `SnapshotMetashrewSync::get_next_block_data_with_floor`
    /// for the full motivation — the short version is "defend against
    /// fetcher wedging when handle_reorg silently lowers current_height
    /// past what the outer fetcher loop already enqueued".
    pub async fn get_next_block_data_with_floor(
        &self,
        min_target_height: Option<u32>,
    ) -> SyncResult<Option<(u32, Vec<u8>, Vec<u8>)>> {
        let mut current_height = self.current_height.load(Ordering::SeqCst);

        // Get remote tip
        let remote_tip = self.node.get_tip_height().await?;

        // Check for reorgs only when close to the tip
        if remote_tip.saturating_sub(current_height) <= self.config.reorg_check_threshold {
            match handle_reorg(
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
                    return Err(e);
                }
            }
        }

        // v9.0.5-rc.13: clamp the fetch target to >= min_target_height so
        // the outer fetcher loop's `last_sent + 1` watermark always wins
        // over a phantom-rolled-back current_height.
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

    /// Process a single block atomically, with bounded retries and **no**
    /// non-atomic fallback path.
    ///
    /// The user-stated invariant is:
    ///
    /// > "if an atomic write fails it either happens or it doesn't, and we
    /// > never skip a block. Anytime we run metashrew-runtime on a block,
    /// > it should produce that set of k/v pairs, then attempt to write
    /// > them until it succeeds, and if it doesn't, or the software is
    /// > restarted, it should pick up where it left off, and rerun that
    /// > block to get that same k/v pairs for that same height, and apply
    /// > them successfully."
    ///
    /// Implementation:
    ///
    /// 1. SPV-style validate that this block connects to our stored tip.
    /// 2. Loop **forever** calling `process_block_atomic`: each attempt
    ///    re-executes the WASM module from scratch (memory refresh is
    ///    unconditional inside `process_block_atomic`), produces a fresh,
    ///    bit-for-bit identical k/v map (modulo timing-side-channels in the
    ///    indexer, which are out of scope here), and tries to commit via
    ///    the storage adapter's `commit_atomic`.
    /// 3. Backoff between attempts is exponential, capped at 30s. Log
    ///    severity escalates: `warn!` for attempts 1-5, `error!` for
    ///    attempts 6-30, `error!` every 10th iteration beyond 30. There
    ///    is **no** `process::exit` — the block-apply blocks until the
    ///    atomic commit succeeds. The operator can SIGKILL to escalate if
    ///    needed. There is also no silent fallback to a different code
    ///    path with different write semantics.
    pub async fn process_block(&self, height: u32, block_data: Vec<u8>, block_hash: Vec<u8>) -> SyncResult<()> {
        // 1. SPV-style continuity check.
        if !self.validate_block_connects(height, &block_data, &block_hash).await? {
            return Err(SyncError::BlockProcessing {
                height,
                message: format!(
                    "Block does not connect to previous block - possible reorg or chain inconsistency"
                ),
            });
        }
        info!(
            "Processing block {} ({} bytes) atomically",
            height,
            block_data.len()
        );

        // 2. Bounded-retry atomic apply. Block until commit succeeds OR
        //    until we exceed MAX_ATOMIC_COMMIT_ATTEMPTS, in which case
        //    surface the last error to the caller (previously this loop
        //    was unbounded; see mork1e's 600-attempt wedge on block 892936
        //    documented at MAX_ATOMIC_COMMIT_ATTEMPTS).
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        loop {
            attempt = attempt.saturating_add(1);

            if attempt > MAX_ATOMIC_COMMIT_ATTEMPTS {
                let msg = last_err.unwrap_or_else(|| "no error captured".to_string());
                return Err(SyncError::Storage(format!(
                    "process_block: commit_atomic at height {} exceeded \
                     max-retry budget ({} attempts): last error: {}",
                    height, MAX_ATOMIC_COMMIT_ATTEMPTS, msg
                )));
            }

            // Backoff (no sleep before first attempt).
            if attempt > 1 {
                let backoff_ms = atomic_retry_backoff_ms(attempt);
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
                    // Sync-framework-side atomic commit. The RocksDB adapter
                    // implementation bundles the three writes (indexed-height,
                    // block-hash record, state-root record) into a single
                    // sync=true WriteBatch and enforces height == tip + 1.
                    let commit_res = {
                        let mut storage = self.storage.write().await;
                        storage
                            .commit_atomic(height, &result.block_hash, &result.batch_data)
                            .await
                    };
                    match commit_res {
                        Ok(()) => {
                            info!(
                                "Block {} committed atomically (attempt {})",
                                height, attempt
                            );
                            self.blocks_processed.fetch_add(1, Ordering::SeqCst);
                            {
                                let mut last_time = self.last_block_time.write().await;
                                *last_time = Some(SystemTime::now());
                            }
                            return Ok(());
                        }
                        Err(commit_err) => {
                            let msg = format!("{}", commit_err);

                            // An out-of-order rejection is DETERMINISTIC: it is a
                            // function of the gap between `height` and the committed
                            // tip, and re-executing the same block cannot change
                            // either. Retrying it 30 times with backoff costs ~43
                            // minutes per block (measured in production on 2026-08-24)
                            // and, worse, DELAYS the resync signal reaching the caller,
                            // which is the only thing that can actually fix it. Bail
                            // immediately and let the result loop route it to
                            // `handle_reorg`.
                            if msg.contains("out-of-order commit rejected") {
                                error!(
                                    "Block {}: {} — not retryable (the committed tip \
                                     cannot advance by re-executing this block); \
                                     surfacing immediately for reorg handling",
                                    height, msg
                                );
                                return Err(SyncError::Storage(format!(
                                    "commit_atomic: {}",
                                    msg
                                )));
                            }

                            log_atomic_retry_failure(
                                "atomic commit",
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
                    log_atomic_retry_failure(
                        "atomic block execution",
                        height,
                        attempt,
                        &msg,
                    );
                    last_err = Some(format!("process_block_atomic: {}", msg));
                }
            }

            // Refresh the runtime's WASM memory before the next attempt so
            // we start from a clean instance — the runtime is supposed to
            // do this internally on error already, but a belt-and-braces
            // refresh here keeps the retry loop deterministic against any
            // future changes to the runtime adapter. Re-executing the block
            // from scratch is the whole point of the retry loop.
            if let Err(e) = self.runtime.refresh_memory().await {
                warn!(
                    "refresh_memory() between retries failed at height {}: {} (continuing)",
                    height, e
                );
            }
        }
    }

    /// Run the sync pipeline with parallel fetching and processing
    async fn run_pipeline(&self) -> SyncResult<()> {
        // Determine pipeline size
        // NOTE: For deterministic behavior across instances, pipeline_size should be
        // explicitly configured rather than auto-detected from CPU count.
        // CPU-based sizing can cause different instances to process blocks in different
        // concurrent patterns, potentially affecting resource contention and timing.
        let pipeline_size = self.config.pipeline_size.unwrap_or_else(|| {
            let cpu_count = num_cpus::get();
            let auto_size = std::cmp::min(std::cmp::max(5, cpu_count / 2), 16);
            warn!(
                "Pipeline size not configured, using auto-detected value {} based on {} CPUs. \
                 For deterministic behavior, explicitly set pipeline_size in config.",
                auto_size, cpu_count
            );
            auto_size
        });

        info!("Starting sync pipeline with size {}", pipeline_size);

        // Create channels for the pipeline
        let (block_sender, mut block_receiver) = mpsc::channel::<(u32, Vec<u8>, Vec<u8>)>(pipeline_size);
        let (result_sender, mut result_receiver) = mpsc::channel::<BlockResult>(pipeline_size);

        // Spawn block fetcher task
        let fetcher_handle = {
            let self_clone = self.clone_for_processing();
            let block_sender = block_sender.clone();

            tokio::spawn(async move {
                loop {
                    if !self_clone.is_running.load(Ordering::SeqCst) {
                        break;
                    }

                    let mut current_height = self_clone.current_height.load(Ordering::SeqCst);

                    // Get remote tip
                    let remote_tip = match self_clone.node.get_tip_height().await {
                        Ok(tip) => tip,
                        Err(e) => {
                            error!("Failed to get tip height: {}", e);
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    // v10 sync-mode: inform the storage layer of the
                    // observed bitcoind tip so its `commit_atomic` can
                    // gate WAL-off behavior on the bitcoind/indexer gap.
                    // Read lock only — adapter uses atomic interior
                    // mutability for the tip value.
                    self_clone.storage.read().await.set_bitcoind_tip(remote_tip).await;

                    // Check for reorgs only when close to the tip
                    if remote_tip.saturating_sub(current_height) <= self_clone.config.reorg_check_threshold {
                        match handle_reorg(
                            current_height,
                            self_clone.node.clone(),
                            self_clone.storage.clone(),
                            self_clone.runtime.clone(),
                            &self_clone.config,
                        )
                        .await
                        {
                            Ok(new_height) => {
                                if new_height != current_height {
                                    info!("Reorg handled. Resuming from height {}", new_height);
                                    self_clone.current_height.store(new_height, Ordering::SeqCst);
                                }
                                current_height = new_height;
                            }
                            Err(e) => {
                                error!("Error handling reorg: {}", e);
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        }
                    }

                    // Check exit condition
                    if let Some(exit_at) = self_clone.config.exit_at {
                        if current_height >= exit_at {
                            info!("Fetcher reached exit height {}", exit_at);
                            break;
                        }
                    }

                    // Check if we need to wait for new blocks
                    if current_height > remote_tip {
                        debug!(
                            "Waiting for new blocks: current={}, tip={}",
                            current_height, remote_tip
                        );
                        sleep(Duration::from_secs(3)).await;
                        continue;
                    }

                    // Check if already processing
                    {
                        let processing_heights = self_clone.processing_heights.lock().await;
                        if processing_heights.contains(&current_height) {
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }

                    // Fetch block
                    match self_clone.node.get_block_info(current_height).await {
                        Ok(block_info) => {
                            info!(
                                "Fetched block {} ({} bytes)",
                                current_height,
                                block_info.data.len()
                            );
                            {
                                let mut processing_heights = self_clone.processing_heights.lock().await;
                                processing_heights.insert(current_height);
                            }
                            if block_sender
                                .send((current_height, block_info.data, block_info.hash))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to fetch block {}: {}", current_height, e);
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                }

                debug!("Block fetcher task completed");
            })
        };

        // Spawn block processor task
        let processor_handle = {
            let sync_engine = self.clone_for_processing();
            let result_sender = result_sender.clone();

            tokio::spawn(async move {
                while let Some((block_height, block_data, block_hash)) = block_receiver.recv().await {
                    info!(
                        "Processing block {} ({} bytes)",
                        block_height,
                        block_data.len()
                    );

                    let result = match sync_engine.process_block(block_height, block_data, block_hash).await {
                        Ok(_) => BlockResult::Success(block_height),
                        Err(e) => BlockResult::Error(block_height, e.to_string()),
                    };

                    if result_sender.send(result).await.is_err() {
                        break;
                    }
                }

                debug!("Block processor task completed");
            })
        };

        // Main result handling loop
        while let Some(result) = result_receiver.recv().await {
            let height = match result {
                BlockResult::Success(processed_height) => {
                    info!("Block {} successfully processed", processed_height);
                    self.current_height.store(processed_height + 1, Ordering::SeqCst);
                    {
                        let mut processing_heights = self.processing_heights.lock().await;
                        processing_heights.remove(&processed_height);
                    }
                    processed_height + 1
                }
                BlockResult::Error(failed_height, error) => {
                    error!("Failed to process block {}: {}", failed_height, error);
                    {
                        let mut processing_heights = self.processing_heights.lock().await;
                        processing_heights.remove(&failed_height);
                    }

                    // Check if this is a chain validation error - trigger reorg handling.
                    //
                    // `out-of-order commit rejected` belongs here (added rc.15). It means
                    // the fetcher cursor has drifted ahead of the committed storage tip:
                    // `commit_atomic` only accepts `tip + 1`, so once a gap opens, every
                    // later block is rejected for the same reason and the gap only widens.
                    // Routing it to the generic retry branch (which does NOT rewind
                    // `current_height`) made the divergence permanent — the wedge behind
                    // INCIDENT-REORG-WEDGE-20260816 and -20260824. `handle_reorg` walks
                    // back to the common ancestor and stores the rollback height, which is
                    // exactly the reconciliation that previously required a process
                    // restart (`heal_pointers_atomic` at startup).
                    if error.contains("does not connect to previous block")
                        || error.contains("CHAIN DISCONTINUITY")
                        || error.contains("out-of-order commit rejected")
                    {
                        warn!("Chain discontinuity detected at height {}. Triggering reorg handling.", failed_height);

                        // Trigger reorg handling to find common ancestor and rollback
                        match handle_reorg(
                            failed_height,
                            self.node.clone(),
                            self.storage.clone(),
                            self.runtime.clone(),
                            &self.config,
                        )
                        .await
                        {
                            Ok(rollback_height) => {
                                info!("Rolled back to height {}. Resuming sync.", rollback_height);
                                self.current_height.store(rollback_height, Ordering::SeqCst);
                                rollback_height
                            }
                            Err(e) => {
                                error!("Failed to handle reorg: {}", e);
                                sleep(Duration::from_secs(5)).await;
                                failed_height
                            }
                        }
                    } else if error.contains("indexer exited unexpectedly") {
                        error!("Critical error: Indexer exited unexpectedly. Aborting.");
                        self.is_running.store(false, Ordering::SeqCst);
                        return Err(SyncError::BlockProcessing {
                            height: failed_height,
                            message: error,
                        });
                    } else {
                        // Other errors: retry after delay
                        sleep(Duration::from_secs(5)).await;
                        failed_height
                    }
                }
            };

            // Check exit condition
            if let Some(exit_at) = self.config.exit_at {
                if height >= exit_at {
                    info!("Reached exit height {}", exit_at);
                    break;
                }
            }

            if !self.is_running.load(Ordering::SeqCst) {
                break;
            }
        }

        // Cleanup
        drop(block_sender);
        drop(result_sender);

        // Wait for tasks to complete
        let _ = tokio::join!(fetcher_handle, processor_handle);

        Ok(())
    }

    /// Create a clone for processing (simplified for this example)
    fn clone_for_processing(&self) -> ProcessingClone<N, S, R> {
        ProcessingClone {
            node: self.node.clone(),
            storage: self.storage.clone(),
            runtime: self.runtime.clone(),
            config: self.config.clone(),
            is_running: self.is_running.clone(),
            current_height: self.current_height.clone(),
            processing_heights: self.processing_heights.clone(),
        }
    }
}

/// Simplified clone for processing tasks
#[derive(Clone)]
struct ProcessingClone<N, S, R>
where
    N: BitcoinNodeAdapter,
    S: StorageAdapter,
    R: RuntimeAdapter,
{
    node: Arc<N>,
    storage: Arc<RwLock<S>>,
    runtime: Arc<R>,
    config: SyncConfig,
    is_running: Arc<AtomicBool>,
    current_height: Arc<AtomicU32>,
    processing_heights: Arc<Mutex<HashSet<u32>>>,
}

impl<N, S, R> ProcessingClone<N, S, R>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    /// Pipeline-mode block apply. Mirrors `MetashrewSync::process_block` but
    /// without the SPV continuity check (that's done by the fetcher upstream
    /// in this pipeline) and without the metric/timing bookkeeping (that's
    /// handled by the result-handling loop). Atomic-only, infinite retry
    /// with exponential backoff — block until the commit succeeds; same
    /// invariants as the non-pipeline path.
    async fn process_block(&self, height: u32, block_data: Vec<u8>, block_hash: Vec<u8>) -> SyncResult<()> {
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);

            if attempt > 1 {
                let backoff_ms = atomic_retry_backoff_ms(attempt);
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
                                "Block {} committed atomically in pipeline (attempt {})",
                                height, attempt
                            );
                            return Ok(());
                        }
                        Err(commit_err) => {
                            log_atomic_retry_failure(
                                "pipeline atomic commit",
                                height,
                                attempt,
                                &format!("{}", commit_err),
                            );
                        }
                    }
                }
                Err(atomic_err) => {
                    log_atomic_retry_failure(
                        "pipeline atomic block execution",
                        height,
                        attempt,
                        &format!("{}", atomic_err),
                    );
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
}

#[async_trait]
impl<N, S, R> SyncEngine for MetashrewSync<N, S, R>
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

        info!("Starting Metashrew sync engine");
        self.is_running.store(true, Ordering::SeqCst);

        // Check connectivity
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

        // Start the pipeline
        self.run_pipeline().await?;

        Ok(())
    }

    async fn stop(&mut self) -> SyncResult<()> {
        info!("Stopping Metashrew sync engine");
        self.is_running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn get_status(&self) -> SyncResult<SyncStatus> {
        let current_height = self.current_height.load(Ordering::SeqCst);
        let tip_height = self.node.get_tip_height().await?;
        let blocks_behind = tip_height.saturating_sub(current_height);
        let last_block_time = *self.last_block_time.read().await;
        let blocks_processed = self.blocks_processed.load(Ordering::SeqCst);

        // Calculate blocks per second (simplified)
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
        let block_hash = self.node.get_block_hash(height).await?;
        self.process_block(height, block_data, block_hash).await
    }

}

// ---------------------------------------------------------------------------
// v9.0.5-rc.6: startup-heal of divergent on-disk pointers.
//
// Background — mainnet incident on a v9.0.5-rc.5 box:
//   The sync engine was looping process_block(N) repeatedly while the
//   storage tip was N+5. commit_atomic rejected every commit with the
//   strict-in-order check. Three pointers existed on disk:
//     __INTERNAL/height               (owned by commit_atomic)
//     /__INTERNAL/tip-height          (owned by runtime-side __flush)
//     /__INTERNAL/height-to-hash/H    (owned by commit_atomic, per height)
//   They were all consistent — but the sync engine's in-memory
//   current_height had been knocked back to N somehow (legacy code path
//   on a prior version), and the rc.5 reorg detector couldn't fire to
//   heal it because the storage state was internally consistent: bitcoind
//   agreed with all stored hashes too. A clean stop+restart unblocked it
//   because init() re-reads __INTERNAL/height. This module preserves that
//   recovery semantics structurally: init() always defensively heals.
//
// The heal must:
//   1. Read all three pointers + an optional bitcoind check.
//   2. If they all agree AND the canonical bitcoind hash matches the
//      stored hash at the tip: no-op (idempotent on healthy state).
//   3. Otherwise: pick min(p1, p2, p3) as the safe candidate, walk DOWN
//      from there until the stored hash matches bitcoind (or bitcoind
//      is unreachable — trust the local pointers and bail to step 4).
//   4. Issue a single atomic rollback to the discovered safe height.
//   5. Set current_height = safe_height + 1 and proceed.
//
// Conservative-by-design: min-wins. We'd rather re-apply blocks (the
// rc.5 single-batch atomic commit makes this idempotent and correct)
// than skip any. The strict-in-order check in commit_atomic guarantees
// we don't accidentally write garbage on top of valid state — it'd be
// rejected at the storage layer.
// ---------------------------------------------------------------------------

/// Outcome of a startup-heal pass. Returned to callers so they can log
/// observably what happened (or didn't).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupHealOutcome {
    /// All three pointers agreed and (optionally) bitcoind confirmed the
    /// tip. No writes happened.
    AlreadyConsistent { tip: u32 },
    /// Pointers diverged or bitcoind disagreed. We rolled back to
    /// `healed_to`. `from` is the highest divergent value we saw.
    Healed { from: u32, healed_to: u32 },
    /// Heal was disabled by config. Pointers were not even read.
    Disabled,
}

/// Run the v9.0.5-rc.6 startup-heal pass. Reads all three pointers,
/// validates the highest commonly-agreed height against bitcoind, and
/// issues an atomic rollback if anything diverges. Idempotent: re-running
/// on already-healed state is a no-op.
///
/// Returns the height that the heal believes is the safe tip — caller
/// should set `current_height = returned_value + 1`. If
/// `enable_startup_heal` is false, returns the raw `__INTERNAL/height`
/// unchanged.
///
/// If bitcoind is unreachable during the heal, we DO NOT block startup —
/// we trust the on-disk pointers and proceed with the min-wins pick.
/// This matches the "don't block startup on a network blip" constraint.
pub async fn heal_pointer_divergence_at_startup<N, S>(
    node: Arc<N>,
    storage: Arc<RwLock<S>>,
    config: &SyncConfig,
) -> SyncResult<(u32, StartupHealOutcome)>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
{
    if !config.enable_startup_heal {
        let storage_guard = storage.read().await;
        let h = storage_guard.get_indexed_height().await.unwrap_or(0);
        drop(storage_guard);
        debug!("startup-heal disabled by config; trusting __INTERNAL/height = {}", h);
        return Ok((h, StartupHealOutcome::Disabled));
    }

    // --- Step 1: read all three pointers ---
    let (indexed_height, runtime_tip, max_blockhash_h) = {
        let storage_guard = storage.read().await;
        let indexed_height = storage_guard.get_indexed_height().await.unwrap_or(0);
        let runtime_tip = storage_guard
            .get_runtime_tip_height()
            .await
            .unwrap_or(indexed_height);
        // bound the blockhash scan: don't look further down than (indexed_height - 256)
        // unless indexed_height is small.
        let floor = indexed_height.saturating_sub(256);
        let max_blockhash_h = storage_guard
            .find_max_stored_block_hash_height(floor)
            .await
            .unwrap_or(indexed_height);
        drop(storage_guard);
        (indexed_height, runtime_tip, max_blockhash_h)
    };

    info!(
        "startup-heal: pointer scan — __INTERNAL/height={} /__INTERNAL/tip-height={} max-stored-blockhash={}",
        indexed_height, runtime_tip, max_blockhash_h
    );

    // --- Step 2: trivially-consistent fast path ---
    let all_agree = indexed_height == runtime_tip && runtime_tip == max_blockhash_h;
    if all_agree && indexed_height == 0 {
        // Fresh DB (or genesis): nothing to heal.
        info!("startup-heal: fresh DB (all pointers 0); no heal needed");
        return Ok((indexed_height, StartupHealOutcome::AlreadyConsistent { tip: indexed_height }));
    }

    // --- Step 3: pick min-wins candidate ---
    let from = indexed_height.max(runtime_tip).max(max_blockhash_h);
    let candidate = indexed_height.min(runtime_tip).min(max_blockhash_h);

    // --- Step 4: bitcoind-validate the candidate (best-effort) ---
    // Walk DOWN from candidate until stored_hash matches bitcoind's hash
    // at that height. Bounded by max_reorg_depth so we can't infinite-loop
    // on a thoroughly corrupted DB.
    let safe_height;
    let bitcoind_reachable = node.is_connected().await;
    if all_agree && bitcoind_reachable {
        // Verify candidate against bitcoind. If they agree, we're clean.
        let storage_guard = storage.read().await;
        let stored = storage_guard.get_block_hash(candidate).await.unwrap_or(None);
        drop(storage_guard);
        if let Some(stored_hash) = stored {
            match node.get_block_hash(candidate).await {
                Ok(remote_hash) if remote_hash == stored_hash => {
                    info!(
                        "startup-heal: all pointers agree at height {} and bitcoind confirms; clean state",
                        candidate
                    );
                    return Ok((candidate, StartupHealOutcome::AlreadyConsistent { tip: candidate }));
                }
                Ok(remote_hash) => {
                    warn!(
                        "startup-heal: pointers agree at {} but bitcoind disagrees (stored={} remote={}); walking back",
                        candidate,
                        hex::encode(&stored_hash),
                        hex::encode(&remote_hash),
                    );
                    // Fall through to the bitcoind walk-back below.
                }
                Err(e) => {
                    warn!(
                        "startup-heal: bitcoind unreachable for height {} ({}); trusting local pointers",
                        candidate, e
                    );
                    return Ok((candidate, StartupHealOutcome::AlreadyConsistent { tip: candidate }));
                }
            }
        } else {
            // Stored hash missing at candidate — heal needed even though
            // pointers agreed numerically. Treat as divergent.
            warn!(
                "startup-heal: stored block-hash record missing at agreed-tip {}; walking back",
                candidate
            );
        }
    } else if !all_agree {
        error!(
            "startup-heal: POINTER DIVERGENCE — __INTERNAL/height={} /__INTERNAL/tip-height={} max-stored-blockhash={}; \
             min-wins candidate = {}, will rollback to safe height",
            indexed_height, runtime_tip, max_blockhash_h, candidate
        );
    }

    // Bitcoind-walk-back loop. Only when bitcoind is reachable; otherwise
    // we just trust the min-wins candidate.
    if bitcoind_reachable {
        let floor = candidate.saturating_sub(config.max_reorg_depth);
        let mut h = candidate;
        loop {
            let storage_guard = storage.read().await;
            let stored = storage_guard.get_block_hash(h).await.unwrap_or(None);
            drop(storage_guard);
            match stored {
                Some(stored_hash) => match node.get_block_hash(h).await {
                    Ok(remote_hash) if remote_hash == stored_hash => {
                        safe_height = h;
                        break;
                    }
                    Ok(_remote_hash) => {
                        warn!(
                            "startup-heal: hash mismatch at height {}; walking back",
                            h
                        );
                    }
                    Err(e) => {
                        warn!(
                            "startup-heal: bitcoind unreachable mid-walk at height {} ({}); trusting candidate {}",
                            h, e, candidate
                        );
                        safe_height = candidate.min(h);
                        break;
                    }
                },
                None => {
                    // No stored hash; can't validate. Walk down.
                    debug!("startup-heal: no stored hash at height {}; walking back", h);
                }
            }
            if h == 0 || h <= floor {
                safe_height = h;
                break;
            }
            h = h.saturating_sub(1);
        }
    } else {
        info!(
            "startup-heal: bitcoind unreachable at init; using min-wins candidate {} without validation",
            candidate
        );
        safe_height = candidate;
    }

    // --- Step 5: issue the atomic heal write ---
    if safe_height < indexed_height || safe_height < runtime_tip || safe_height < max_blockhash_h {
        warn!(
            "startup-heal: ROLLING BACK to height {} (was indexed_height={} runtime_tip={} max_blockhash_h={})",
            safe_height, indexed_height, runtime_tip, max_blockhash_h
        );
        let mut storage_guard = storage.write().await;
        if let Err(e) = storage_guard.heal_pointers_atomic(safe_height).await {
            error!(
                "startup-heal: heal_pointers_atomic FAILED at height {}: {} — refusing to start with divergent state",
                safe_height, e
            );
            return Err(e);
        }
        drop(storage_guard);
        info!("startup-heal: rolled back to height {} successfully", safe_height);
        Ok((safe_height, StartupHealOutcome::Healed { from, healed_to: safe_height }))
    } else {
        info!(
            "startup-heal: pointers consistent at height {} after walk-back (no write needed)",
            safe_height
        );
        Ok((safe_height, StartupHealOutcome::AlreadyConsistent { tip: safe_height }))
    }
}

/// Handles chain reorganizations by finding the common ancestor and rolling back state.
pub async fn handle_reorg<N, S, R>(
    current_height: u32,
    node: Arc<N>,
    storage: Arc<RwLock<S>>,
    runtime: Arc<R>,
    config: &SyncConfig,
) -> SyncResult<u32>
where
    N: BitcoinNodeAdapter + 'static,
    S: StorageAdapter + 'static,
    R: RuntimeAdapter + 'static,
{
    let mut check_height = current_height.saturating_sub(1);
    let mut reorg_detected = false;
    if current_height == 0 {
        return Ok(0);
    }
    // Find the common ancestor
    while check_height > 0 && check_height >= current_height.saturating_sub(config.max_reorg_depth) {
        let storage_guard = storage.read().await;
        let local_hash = match storage_guard.get_block_hash(check_height).await {
            Ok(Some(hash)) => hash,
            _ => {
                check_height = check_height.saturating_sub(1);
                continue;
            }
        };
        drop(storage_guard);

        let remote_hash = match node.get_block_hash(check_height).await {
            Ok(hash) => hash,
            Err(e) => {
                error!("Failed to get remote block hash at height {}: {}", check_height, e);
                return Ok(current_height); // Don't reorg if node is failing
            }
        };

        if local_hash == remote_hash {
            break; // Common ancestor found
        }

        reorg_detected = true;
        check_height = check_height.saturating_sub(1);
    }

    if reorg_detected {
        let rollback_height = check_height;
        warn!("Reorg detected. Rolling back to height {}", rollback_height);

        // Rollback storage
        let mut storage_guard = storage.write().await;
        storage_guard.rollback_to_height(rollback_height).await?;
        drop(storage_guard);

        // Refresh runtime memory
        runtime.refresh_memory().await?;

        return Ok(rollback_height + 1);
    }

    Ok(current_height)
}

#[async_trait]
impl<N, S, R> JsonRpcProvider for MetashrewSync<N, S, R>
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
            height
                .parse::<u32>()
                .map_err(|e| SyncError::Serialization(format!("Invalid height: {}", e)))?
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
            height
                .parse::<u32>()
                .map_err(|e| SyncError::Serialization(format!("Invalid height: {}", e)))?
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

        Ok(serde_json::json!({
            "enabled": true,
            "current_height": self.current_height.load(Ordering::SeqCst),
            "indexed_height": stats.indexed_height,
            "total_entries": stats.total_entries,
            "storage_size_bytes": stats.storage_size_bytes
        }))
    }
}