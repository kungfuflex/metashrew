//! # Core Trait Definitions for the Synchronization Framework
//!
//! This module defines the essential traits that enable the modular architecture
//! of the rockshrew-sync framework. These traits provide clean abstractions for
//! different components of a Bitcoin indexer, allowing for pluggable implementations
//! and comprehensive testing.
//!
//! ## Adapter Traits
//!
//! The framework uses the Adapter pattern to abstract external dependencies:
//!
//! ### [`BitcoinNodeAdapter`]
//! Abstracts communication with Bitcoin nodes, supporting various interfaces:
//! - **Bitcoin Core RPC**: Standard JSON-RPC interface
//! - **REST APIs**: HTTP-based block and transaction retrieval
//! - **Custom protocols**: Specialized node communication methods
//!
//! ### [`StorageAdapter`]
//! Abstracts persistent storage operations, supporting multiple backends:
//! - **RocksDB**: High-performance embedded database
//! - **PostgreSQL**: Relational database with ACID properties
//! - **Custom storage**: Application-specific storage solutions
//!
//! ### [`RuntimeAdapter`]
//! Abstracts WASM runtime execution for indexer modules:
//! - **Metashrew runtime**: Production WASM execution environment
//! - **Mock runtime**: Testing and development environment
//! - **Custom runtimes**: Specialized execution environments
//!
//! ## Service Traits
//!
//! ### [`JsonRpcProvider`]
//! Defines the external API interface for accessing indexed data:
//! - **View functions**: Query indexed data at specific heights
//! - **Preview functions**: Test indexing logic with hypothetical blocks
//! - **Metadata access**: Retrieve indexer status and configuration
//!
//! ### [`SyncEngine`]
//! Coordinates all components for complete blockchain synchronization:
//! - **Orchestration**: Manages the interaction between all adapters
//! - **State management**: Tracks synchronization progress and status
//! - **Error handling**: Provides robust error recovery mechanisms
//!
//! ## Design Principles
//!
//! ### Async-First
//! All traits use `async_trait` to support:
//! - **Non-blocking I/O**: Efficient resource utilization
//! - **Concurrent processing**: Parallel block processing capabilities
//! - **Scalable architecture**: Support for high-throughput indexing
//!
//! ### Error Handling
//! Comprehensive error handling through [`SyncResult`]:
//! - **Typed errors**: Specific error types for different failure modes
//! - **Error propagation**: Clean error bubbling through the call stack
//! - **Recovery strategies**: Information needed for error recovery
//!
//! ### Testability
//! Traits designed for easy testing:
//! - **Mock implementations**: Built-in mock adapters for testing
//! - **Dependency injection**: Easy substitution of components
//! - **Isolated testing**: Test individual components in isolation

use crate::{BlockInfo, ChainTip, PreviewCall, SyncResult, ViewCall, ViewResult};
use async_trait::async_trait;

/// Trait for Bitcoin node adapters that provide blockchain data.
///
/// This trait abstracts the interface to Bitcoin nodes, allowing the synchronization
/// framework to work with different node implementations and communication protocols.
/// Implementations can use Bitcoin Core RPC, REST APIs, or custom protocols.
///
/// # Implementation Requirements
///
/// Implementations must ensure:
/// - **Consistency**: Block hashes and data must be consistent across calls
/// - **Reliability**: Handle network failures gracefully with appropriate retries
/// - **Performance**: Optimize for batch operations when possible
/// - **Thread safety**: Support concurrent access from multiple threads
///
/// # Error Handling
///
/// Methods should return [`SyncError::BitcoinNode`] for node-related failures:
/// - Network connectivity issues
/// - Invalid block heights or hashes
/// - Node synchronization problems
/// - RPC/API errors
#[async_trait]
pub trait BitcoinNodeAdapter: Send + Sync {
    /// Get the current blockchain tip height.
    ///
    /// Returns the height of the most recent block in the node's active chain.
    /// This is used to determine how many blocks need to be processed during
    /// synchronization.
    ///
    /// # Returns
    /// The current tip height as a 32-bit unsigned integer
    ///
    /// # Errors
    /// Returns [`SyncError::BitcoinNode`] if:
    /// - Node is unreachable or unresponsive
    /// - Node is not fully synchronized
    /// - RPC/API call fails
    async fn get_tip_height(&self) -> SyncResult<u32>;

    /// Get the hash of a block at a specific height.
    ///
    /// Retrieves the block hash for the block at the given height in the
    /// active chain. This is used for chain reorganization detection and
    /// block validation.
    ///
    /// # Parameters
    /// - `height`: Block height to retrieve hash for
    ///
    /// # Returns
    /// 32-byte block hash as a vector
    ///
    /// # Errors
    /// Returns [`SyncError::BitcoinNode`] if:
    /// - Height is beyond the current tip
    /// - Height is negative or invalid
    /// - Node communication fails
    async fn get_block_hash(&self, height: u32) -> SyncResult<Vec<u8>>;

    /// Get the raw block data at a specific height.
    ///
    /// Retrieves the complete serialized block data for processing by the
    /// indexer runtime. The data should be in Bitcoin's standard serialization
    /// format.
    ///
    /// # Parameters
    /// - `height`: Block height to retrieve data for
    ///
    /// # Returns
    /// Raw block data as serialized bytes
    ///
    /// # Errors
    /// Returns [`SyncError::BitcoinNode`] if:
    /// - Block at height doesn't exist
    /// - Block data is corrupted or invalid
    /// - Network or storage error on node
    async fn get_block_data(&self, height: u32) -> SyncResult<Vec<u8>>;

    /// Get complete block information at a specific height.
    ///
    /// This is a convenience method that combines hash and data retrieval
    /// into a single operation. Implementations may optimize this for
    /// better performance compared to separate calls.
    ///
    /// # Parameters
    /// - `height`: Block height to retrieve information for
    ///
    /// # Returns
    /// [`BlockInfo`] containing height, hash, and raw data
    ///
    /// # Performance
    /// Implementations should optimize this method for efficiency, potentially
    /// using batch RPC calls or caching to reduce network overhead.
    async fn get_block_info(&self, height: u32) -> SyncResult<BlockInfo>;

    /// Get the current chain tip information.
    ///
    /// Returns both the height and hash of the current chain tip. This is
    /// more efficient than separate calls when both pieces of information
    /// are needed.
    ///
    /// # Returns
    /// [`ChainTip`] containing current height and hash
    ///
    /// # Usage
    /// Used for:
    /// - Initial synchronization planning
    /// - Reorg detection by comparing with stored tips
    /// - Progress monitoring during sync
    async fn get_chain_tip(&self) -> SyncResult<ChainTip>;

    /// Check if the node is reachable and responsive.
    ///
    /// Performs a lightweight check to verify node connectivity without
    /// retrieving significant data. This is used for health monitoring
    /// and connection management.
    ///
    /// # Returns
    /// `true` if node is reachable and responsive, `false` otherwise
    ///
    /// # Implementation Notes
    /// - Should be fast and lightweight (e.g., ping or getinfo call)
    /// - Should not throw errors, only return boolean status
    /// - May cache results briefly to avoid excessive network calls
    async fn is_connected(&self) -> bool;
}

/// Trait for storage adapters that persist indexed data
#[async_trait]
pub trait StorageAdapter: Send + Sync {
    /// Get the current indexed height
    async fn get_indexed_height(&self) -> SyncResult<u32>;

    /// Set the current indexed height
    async fn set_indexed_height(&mut self, height: u32) -> SyncResult<()>;

    /// Store a block hash for a given height
    async fn store_block_hash(&mut self, height: u32, hash: &[u8]) -> SyncResult<()>;

    /// Get a stored block hash for a given height
    async fn get_block_hash(&self, height: u32) -> SyncResult<Option<Vec<u8>>>;

    /// Store a state root for a given height
    async fn store_state_root(&mut self, height: u32, root: &[u8]) -> SyncResult<()>;

    /// Get a state root for a given height
    async fn get_state_root(&self, height: u32) -> SyncResult<Option<Vec<u8>>>;

    /// Rollback storage to a specific height (remove data after this height)
    async fn rollback_to_height(&mut self, height: u32) -> SyncResult<()>;

    /// Check if storage is available and writable
    async fn is_available(&self) -> bool;

    /// Get storage statistics (size, entries, etc.)
    async fn get_stats(&self) -> SyncResult<StorageStats>;

    /// Get the underlying database handle for snapshot operations
    /// This is specific to RocksDB implementations and may not be available for all storage adapters
    async fn get_db_handle(&self) -> SyncResult<std::sync::Arc<rocksdb::DB>> {
        Err(crate::SyncError::Storage(
            "Database handle not available for this storage adapter".to_string(),
        ))
    }

    /// v9.0.5-rc.6 startup-heal hook: read the runtime-owned tip-height
    /// pointer (`/__INTERNAL/tip-height`). This is the pointer written by
    /// the WASM-runtime side's `__flush` and `handle_reorg`. In a healthy
    /// system this equals `get_indexed_height`, but on a DB left behind
    /// by a crash-looping pre-rc.5 build they can diverge — the runtime
    /// flushed its tip but `commit_atomic` was rejected, or vice versa.
    ///
    /// Default impl returns `get_indexed_height` (so mock-style adapters
    /// that don't track the runtime tip key separately can't trip the
    /// divergence-detection code). RocksDB-backed adapters MUST override.
    async fn get_runtime_tip_height(&self) -> SyncResult<u32> {
        self.get_indexed_height().await
    }

    /// v9.0.5-rc.6 startup-heal hook: walk down from `get_indexed_height`
    /// and return the highest height H for which a block-hash record
    /// exists (i.e. `get_block_hash(H)` returns Some). Bounded by
    /// `floor` so we don't scan the entire chain on a damaged DB.
    ///
    /// Default impl uses a bounded linear walk via `get_block_hash`. The
    /// RocksDB adapter can override with a prefix scan for speed.
    async fn find_max_stored_block_hash_height(&self, floor: u32) -> SyncResult<u32> {
        let top = self.get_indexed_height().await?;
        if top == 0 {
            return Ok(0);
        }
        let mut h = top;
        let stop_at = floor;
        while h > stop_at {
            if self.get_block_hash(h).await?.is_some() {
                return Ok(h);
            }
            h = h.saturating_sub(1);
        }
        // h == floor or 0: check it explicitly
        if self.get_block_hash(h).await?.is_some() {
            Ok(h)
        } else {
            Ok(0)
        }
    }

    /// v9.0.5-rc.6 startup-heal hook: bring all three pointers into line
    /// with `target_height` in ONE atomic batch — rollback storage past
    /// the target AND write the runtime-side tip-height key. RocksDB
    /// adapters that have direct DB access should override to use a
    /// single `WriteBatch` (matching the rc.5 single-batch invariant);
    /// the default impl falls back to `rollback_to_height` followed by
    /// no runtime-tip write (mock adapters don't have a separate runtime
    /// pointer).
    async fn heal_pointers_atomic(&mut self, target_height: u32) -> SyncResult<()> {
        let current = self.get_indexed_height().await?;
        if current > target_height {
            self.rollback_to_height(target_height).await?;
        } else if current < target_height {
            return Err(crate::SyncError::Storage(format!(
                "heal_pointers_atomic refused to advance tip: current={} target={} \
                 (heal only rolls back, never forward)",
                current, target_height
            )));
        }
        Ok(())
    }

    /// Atomically commit a block in a SINGLE underlying write: the WASM-side
    /// per-block batch (`batch_data`, produced by `process_block_atomic` and
    /// shipped through `AtomicBlockResult::batch_data`) AND the sync-framework
    /// metadata writes (indexed-height pointer, block-hash record, state-root
    /// record) are submitted together — one `db.write_opt(batch, sync=true)`,
    /// one fsync.
    ///
    /// # Invariants enforced by the implementation
    ///
    /// - **Strict in-order progression**: the call MUST fail (and write nothing) when
    ///   `height != current_indexed_height + 1`. This matches the user-stated invariant
    ///   that `tip_height` advances exactly 0 → 1 → 2 → … with no gaps, no skips, and
    ///   no rewrites outside of an explicit `rollback_to_height` call.
    /// - **True all-or-nothing**: WASM writes + metadata writes land in one
    ///   underlying batch with sync/fsync, so a crash leaves the DB at either
    ///   `tip = height - 1` (nothing committed) or `tip = height` (everything
    ///   committed). There is no intermediate state where the WASM writes
    ///   landed but the metadata didn't, or vice versa. This closes the
    ///   two-fsync window present in pre-rc.4 builds, which is the class of
    ///   bug consistent with the supply-drift observed on mainnet g/h pods.
    ///
    /// The default implementation here is a *non-atomic* shim that calls the three
    /// individual mutators in sequence and ignores `batch_data`. It exists only so
    /// that legacy / mock storage adapters keep compiling — RocksDB-backed adapters
    /// MUST override it to get the atomicity + durability guarantees described above.
    async fn commit_atomic(
        &mut self,
        height: u32,
        block_hash: &[u8],
        state_root: &[u8],
        _batch_data: &[u8],
    ) -> SyncResult<()> {
        // Strict in-order check (default impl): refuse to commit out of sequence.
        let current = self.get_indexed_height().await?;
        // If current is 0 and we have no stored hash for height 0, we accept any first
        // commit (genesis or configured start_block). Otherwise the new height must be
        // exactly current + 1.
        let allow_first = current == 0 && self.get_block_hash(0).await?.is_none();
        if !allow_first && height != current.saturating_add(1) {
            return Err(crate::SyncError::Storage(format!(
                "out-of-order commit rejected: attempted height {} but current tip is {} \
                 (commit_atomic only accepts height == tip + 1)",
                height, current
            )));
        }
        self.store_block_hash(height, block_hash).await?;
        self.store_state_root(height, state_root).await?;
        self.set_indexed_height(height).await?;
        Ok(())
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_entries: usize,
    pub indexed_height: u32,
    pub storage_size_bytes: Option<u64>,
}

/// Trait for runtime adapters that execute WASM indexer modules
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    /// Process a block with the WASM indexer
    async fn process_block(&self, height: u32, block_data: &[u8]) -> SyncResult<()>;

    /// Process a block atomically, returning all database operations in a batch
    /// This ensures atomicity by collecting all operations before committing
    async fn process_block_atomic(
        &self,
        height: u32,
        block_data: &[u8],
        block_hash: &[u8],
    ) -> SyncResult<AtomicBlockResult>;

    /// Execute a view function
    async fn execute_view(&self, call: ViewCall) -> SyncResult<ViewResult>;

    /// Execute a preview function (with block data)
    async fn execute_preview(&self, call: PreviewCall) -> SyncResult<ViewResult>;

    /// Get the state root at a specific height
    async fn get_state_root(&self, height: u32) -> SyncResult<Vec<u8>>;

    /// Refresh the runtime memory (cleanup/reset)
    async fn refresh_memory(&self) -> SyncResult<()>;

    /// Check if the runtime is ready for processing
    async fn is_ready(&self) -> bool;

    /// Get runtime statistics
    async fn get_stats(&self) -> SyncResult<RuntimeStats>;

    /// Track runtime updates for snapshot creation (optional, used in snapshot mode)
    /// This method should be called after successful block processing to capture
    /// key-value changes for snapshot diff generation
    async fn track_runtime_updates(&self, _height: u32) -> SyncResult<()> {
        // Default implementation does nothing - only MetashrewRuntimeAdapter implements this
        Ok(())
    }
}

/// Result of atomic block processing containing all operations to be committed
#[derive(Debug, Clone)]
pub struct AtomicBlockResult {
    /// The state root calculated after processing
    pub state_root: Vec<u8>,
    /// All database operations as a serialized batch
    pub batch_data: Vec<u8>,
    /// Block height that was processed
    pub height: u32,
    /// Block hash
    pub block_hash: Vec<u8>,
}

/// Runtime statistics
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    pub memory_usage_bytes: usize,
    pub blocks_processed: u32,
    pub last_refresh_height: Option<u32>,
}

/// Trait for JSON-RPC API providers
#[async_trait]
pub trait JsonRpcProvider: Send + Sync {
    /// Execute a view function call
    async fn metashrew_view(
        &self,
        function_name: String,
        input_hex: String,
        height: String,
    ) -> SyncResult<String>;

    /// Execute a preview function call
    async fn metashrew_preview(
        &self,
        block_hex: String,
        function_name: String,
        input_hex: String,
        height: String,
    ) -> SyncResult<String>;

    /// Get the current indexed height
    async fn metashrew_height(&self) -> SyncResult<u32>;

    /// Get a block hash by height
    async fn metashrew_getblockhash(&self, height: u32) -> SyncResult<String>;

    /// Get a state root by height
    async fn metashrew_stateroot(&self, height: String) -> SyncResult<String>;

    /// Get snapshot information
    async fn metashrew_snapshot(&self) -> SyncResult<serde_json::Value>;
}

/// Trait for the complete sync engine that coordinates all components
#[async_trait]
pub trait SyncEngine: Send + Sync {
    /// Start the synchronization process
    async fn start(&mut self) -> SyncResult<()>;

    /// Stop the synchronization process
    async fn stop(&mut self) -> SyncResult<()>;

    /// Get the current sync status
    async fn get_status(&self) -> SyncResult<SyncStatus>;

    /// Process a single block (for testing)
    async fn process_single_block(&mut self, height: u32) -> SyncResult<()>;
}

/// Sync engine status
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub is_running: bool,
    pub current_height: u32,
    pub tip_height: u32,
    pub blocks_behind: u32,
    pub last_block_time: Option<std::time::SystemTime>,
    pub blocks_per_second: f64,
}
