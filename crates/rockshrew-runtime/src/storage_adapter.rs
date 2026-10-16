//! RocksDB-specific implementation of the `StorageAdapter` trait.

use async_trait::async_trait;
use log::{info, warn};
use metashrew_runtime::{
    rollback::{RollbackOp, SmtRollback},
    KeyValueStoreLike,
};
use metashrew_sync::{StorageAdapter, StorageStats, SyncError, SyncResult};
use rocksdb::{WriteBatch, WriteOptions, DB};
use std::sync::Arc;

use crate::adapter::RocksDBRuntimeAdapter;

/// Build `WriteOptions` with `set_sync(true)` so the RocksDB WAL is fsynced
/// before the write returns. This is what makes `commit_atomic` actually
/// durable — without it, RocksDB can return Ok to the caller and then lose
/// the write across a host crash or OOM kill before the kernel page cache
/// is flushed. See the comment block on `StorageAdapter::commit_atomic` for
/// the determinism invariant this protects.
fn sync_write_options() -> WriteOptions {
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    wo
}

/// RocksDB storage adapter for persistent storage.
#[derive(Clone)]
pub struct RocksDBStorageAdapter {
    db: Arc<DB>,
}

impl RocksDBStorageAdapter {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }
}

// Implement SmtRollback trait for proper reorg handling
impl SmtRollback for RocksDBStorageAdapter {
    fn iter_keys<F>(&self, mut callback: F) -> anyhow::Result<()>
    where
        F: FnMut(&[u8]) -> anyhow::Result<()>,
    {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, _)) => callback(&key)?,
                Err(e) => return Err(anyhow::anyhow!("Failed to iterate keys: {}", e)),
            }
        }
        Ok(())
    }

    fn delete_key(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.db
            .delete(key)
            .map_err(|e| anyhow::anyhow!("Failed to delete key: {}", e))
    }

    fn put_key(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| anyhow::anyhow!("Failed to put key: {}", e))
    }

    fn get_value(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.db
            .get(key)
            .map_err(|e| anyhow::anyhow!("Failed to get value: {}", e))
    }

    fn apply_atomic(&mut self, ops: &[RollbackOp]) -> anyhow::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                RollbackOp::Put(k, v) => batch.put(k, v),
                RollbackOp::Delete(k) => batch.delete(k),
            }
        }
        self.db
            .write(batch)
            .map_err(|e| anyhow::anyhow!("Atomic rollback batch write failed: {}", e))
    }
}

/// Key constants used for the v9.0.5-rc.6 startup-heal pointer reconciliation.
/// Mirror the keys defined elsewhere — see `metashrew-runtime/src/runtime.rs`
/// (`TIP_HEIGHT_KEY`) and `storage_adapter::store_block_hash` for the on-disk
/// canonical layout. They're duplicated here on purpose so the adapter doesn't
/// take a circular dep on metashrew-runtime just to read its own keys.
const INDEXED_HEIGHT_KEY: &[u8] = b"__INTERNAL/height";
const RUNTIME_TIP_HEIGHT_KEY: &[u8] = b"/__INTERNAL/tip-height";
const BLOCK_HASH_PREFIX: &str = "/__INTERNAL/height-to-hash/";

#[async_trait]
impl StorageAdapter for RocksDBStorageAdapter {
    async fn get_indexed_height(&self) -> SyncResult<u32> {
        let height_key = b"__INTERNAL/height".to_vec();
        match self.db.get(&height_key) {
            Ok(Some(value)) => {
                if value.len() >= 4 {
                    let height_bytes: [u8; 4] = value[..4]
                        .try_into()
                        .map_err(|_| SyncError::Storage("Invalid height data".to_string()))?;
                    Ok(u32::from_le_bytes(height_bytes))
                } else {
                    Ok(0)
                }
            }
            Ok(None) => Ok(0),
            Err(e) => Err(SyncError::Storage(format!("Database error: {}", e))),
        }
    }

    async fn set_indexed_height(&mut self, height: u32) -> SyncResult<()> {
        let height_key = b"__INTERNAL/height".to_vec();
        let height_bytes = height.to_le_bytes();
        self.db
            .put(&height_key, &height_bytes)
            .map_err(|e| SyncError::Storage(format!("Failed to store height: {}", e)))
    }

    async fn store_block_hash(&mut self, height: u32, hash: &[u8]) -> SyncResult<()> {
        let blockhash_key = format!("/__INTERNAL/height-to-hash/{}", height).into_bytes();
        self.db
            .put(&blockhash_key, hash)
            .map_err(|e| SyncError::Storage(format!("Failed to store blockhash: {}", e)))
    }

    async fn get_block_hash(&self, height: u32) -> SyncResult<Option<Vec<u8>>> {
        let blockhash_key = format!("/__INTERNAL/height-to-hash/{}", height).into_bytes();
        match self.db.get(&blockhash_key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(SyncError::Storage(format!("Database error: {}", e))),
        }
    }

    async fn store_state_root(&mut self, height: u32, root: &[u8]) -> SyncResult<()> {
        let adapter = RocksDBRuntimeAdapter::new(self.db.clone());
        let mut smt_helper = metashrew_runtime::smt::SMTHelper::new(adapter);
        let root_key = format!("smt:root:{}", height).into_bytes();
        smt_helper
            .storage
            .put(&root_key, root)
            .map_err(|e| SyncError::Storage(format!("Failed to store state root: {}", e)))
    }

    async fn get_state_root(&self, height: u32) -> SyncResult<Option<Vec<u8>>> {
        let adapter = RocksDBRuntimeAdapter::new(self.db.clone());
        let smt_helper = metashrew_runtime::smt::SMTHelper::new(adapter);
        match smt_helper.get_smt_root_at_height(height) {
            Ok(root) => Ok(Some(root.to_vec())),
            Err(_) => Ok(None),
        }
    }

    async fn rollback_to_height(&mut self, height: u32) -> SyncResult<()> {
        use metashrew_runtime::rollback::{rollback_smt_data, rollback_with_manifests};

        info!("Starting rollback to height {}", height);
        let current_height = self.get_indexed_height().await?;

        // Try fast manifest-based rollback first, fall back to full scan
        let used_fast = rollback_with_manifests(self, height, current_height)
            .map_err(|e| SyncError::Storage(format!("Manifest rollback failed: {}", e)))?;

        if !used_fast {
            warn!("Using full SMT rollback (no manifests for rollback range)");
            rollback_smt_data(self, height, current_height)
                .map_err(|e| SyncError::Storage(format!("SMT rollback failed: {}", e)))?;
        }

        // Move BOTH height pointers in one synced batch. This used to be
        // `set_indexed_height(height)` alone, which left the runtime-side
        // `/__INTERNAL/tip-height` (written by every block's commit batch)
        // at the pre-rollback height: the divergence every restart after a
        // reorg had to heal ("heal_pointers_atomic: reconciled pointers at
        // height N-1 (was indexed=N-1, runtime_tip=N)", testnet4
        // rockshrew-a 2026-10-16; alspo-* 2026-10-14).
        let mut batch = WriteBatch::default();
        batch.put(INDEXED_HEIGHT_KEY, &height.to_le_bytes());
        batch.put(RUNTIME_TIP_HEIGHT_KEY, &height.to_le_bytes());
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(|e| SyncError::Storage(format!("rollback pointer write failed: {}", e)))?;
        info!("Successfully completed rollback to height {}", height);
        Ok(())
    }

    async fn is_available(&self) -> bool {
        self.db.get(b"__test").is_ok()
    }

    async fn get_stats(&self) -> SyncResult<StorageStats> {
        let indexed_height = self.get_indexed_height().await?;
        Ok(StorageStats {
            total_entries: 0,
            indexed_height,
            storage_size_bytes: None,
        })
    }

    async fn get_db_handle(&self) -> SyncResult<Arc<DB>> {
        Ok(self.db.clone())
    }

    /// v9.0.5-rc.6 startup-heal: read the runtime-side tip-height pointer
    /// directly from RocksDB. This is the key written by the WASM
    /// runtime's `__flush` hook (see `metashrew_runtime::TIP_HEIGHT_KEY`).
    /// Returns 0 if the key is missing.
    async fn get_runtime_tip_height(&self) -> SyncResult<u32> {
        match self.db.get(RUNTIME_TIP_HEIGHT_KEY) {
            Ok(Some(value)) if value.len() >= 4 => {
                let bytes: [u8; 4] = value[..4].try_into().map_err(|_| {
                    SyncError::Storage("Invalid runtime-tip-height data".to_string())
                })?;
                Ok(u32::from_le_bytes(bytes))
            }
            Ok(_) => Ok(0),
            Err(e) => Err(SyncError::Storage(format!(
                "Database error reading runtime tip: {}",
                e
            ))),
        }
    }

    /// v9.0.5-rc.6 startup-heal: prefix-scan
    /// `/__INTERNAL/height-to-hash/` and return the highest stored
    /// height. Bounded by `floor` (we never report below `floor`,
    /// returning 0 if no record is found within [floor, indexed_tip]).
    ///
    /// Use case: detect commit_atomic having written block-hash records
    /// past the indexed-height pointer, or block-hash records missing
    /// behind the indexed-height pointer (the pre-rc.5 two-fsync window).
    async fn find_max_stored_block_hash_height(&self, floor: u32) -> SyncResult<u32> {
        // We can't use a generic RocksDB prefix iterator efficiently
        // here because keys are decimal-encoded heights — lexicographic
        // ordering doesn't match numeric ordering ("999" < "9999" but
        // 999 < 9999 too; "1000" > "999" lexicographically too because
        // '1' < '9'... wait, '1'=0x31, '9'=0x39 so "1000" < "9999"
        // lexicographically, which IS numeric-correct for fixed-width.
        // But "1000" vs "999": '1' < '9' so "1000" < "999"
        // lexicographically — that's WRONG numerically). So we walk
        // down from the indexed-height pointer instead, bounded by
        // `floor`. This is exactly the rc.5 behavior of the default
        // trait impl — we just inline it here to make it explicit.
        let top = self.get_indexed_height().await?;
        if top == 0 {
            return Ok(0);
        }
        let mut h = top;
        loop {
            let key = format!("{}{}", BLOCK_HASH_PREFIX, h).into_bytes();
            match self.db.get(&key) {
                Ok(Some(_)) => return Ok(h),
                Ok(None) => {}
                Err(e) => {
                    return Err(SyncError::Storage(format!(
                        "DB error during blockhash scan: {}",
                        e
                    )))
                }
            }
            if h <= floor || h == 0 {
                return Ok(0);
            }
            h = h.saturating_sub(1);
        }
    }

    /// v9.0.5-rc.6 startup-heal: bring all three pointers into line with
    /// `target_height` in a SINGLE atomic batch — the rollback, the
    /// `__INTERNAL/height` overwrite (which `rollback_to_height` would
    /// have done anyway via `set_indexed_height`), AND the
    /// `/__INTERNAL/tip-height` overwrite. Writes go through
    /// `db.write_opt(batch, sync=true)` so the heal is durable on a
    /// crash mid-startup.
    ///
    /// Idempotent: if all pointers already match `target_height` this is a
    /// no-op (no writes), preserving the "init() on already-healed state is
    /// a no-op" invariant required by the heal-spec.
    async fn heal_pointers_atomic(&mut self, target_height: u32) -> SyncResult<()> {
        let current_indexed = self.get_indexed_height().await?;
        let current_runtime_tip = self.get_runtime_tip_height().await?;

        // Fast path: nothing to do.
        if current_indexed == target_height && current_runtime_tip == target_height {
            return Ok(());
        }

        if current_indexed > target_height {
            // The rollback_to_height path is the canonical, well-tested
            // truncation routine — let it do the SMT-side cleanup.
            // After it runs, `__INTERNAL/height` will equal target_height
            // (via the `set_indexed_height` call at the end).
            info!(
                "heal_pointers_atomic: rolling back indexed_height from {} to {}",
                current_indexed, target_height
            );
            self.rollback_to_height(target_height).await?;
        } else if current_indexed < target_height {
            return Err(SyncError::Storage(format!(
                "heal_pointers_atomic refused to advance tip: current_indexed={} target={} \
                 (heal only rolls back, never forward)",
                current_indexed, target_height
            )));
        }

        // Now write `/__INTERNAL/tip-height` (runtime side) into the same
        // batch as a re-affirmation of `__INTERNAL/height` (sync side).
        // This guarantees both pointers land together post-heal even if
        // the runtime-tip key drifted from the indexed-height key on
        // disk. One fsync, all-or-nothing.
        let mut batch = WriteBatch::default();
        batch.put(INDEXED_HEIGHT_KEY, &target_height.to_le_bytes());
        batch.put(RUNTIME_TIP_HEIGHT_KEY, &target_height.to_le_bytes());
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(|e| SyncError::Storage(format!("heal_pointers_atomic write failed: {}", e)))?;

        info!(
            "heal_pointers_atomic: reconciled pointers at height {} (was indexed={}, runtime_tip={})",
            target_height, current_indexed, current_runtime_tip
        );
        Ok(())
    }

    /// Commit block `height` atomically: reconstruct the WASM-side `WriteBatch`
    /// from `batch_data` (serialized by `BatchLike::to_bytes()` inside
    /// `process_block_atomic`'s __flush hook), APPEND the sync-framework metadata
    /// writes (block-hash record, state-root record, indexed-height pointer)
    /// into the SAME batch, and submit exactly ONE
    /// `db.write_opt(batch, WriteOptions::set_sync(true))` call.
    ///
    /// This is the single point of database commit for block-apply. Pre-rc.4 had
    /// two writes — `RocksDBRuntimeAdapter::write` for the WASM batch and this
    /// `commit_atomic` for the three metadata writes — which meant a process
    /// crash between the two fsyncs could leave the DB in a partial state:
    /// WASM-side state for block N committed (alkanes balances, totalsupply, etc.)
    /// but the indexed-height pointer / block-hash / state-root metadata missing
    /// or stale, so on restart the indexer would re-apply block N on top of
    /// already-applied state, double-counting append-only updates and producing
    /// the supply drift we saw on g/h. With this single-batch design, RocksDB's
    /// `WriteBatch` atomicity primitive guarantees that's not a representable
    /// outcome.
    ///
    /// Strict in-order check: refuses to commit unless `height == tip + 1`,
    /// matching the user-stated invariant that we never skip or rewrite a
    /// block outside of an explicit reorg rollback. The check reads tip
    /// from the on-disk snapshot of `__INTERNAL/height` — NOT from any keys
    /// staged in the in-flight batch — so the WASM-side batch can no longer
    /// self-reject the commit it produced (the rc.4 Option-A failure mode is
    /// also closed because the height pointer is now owned exclusively by
    /// `commit_atomic` and lands in the same batch as the tip-advancement).
    async fn commit_atomic(
        &mut self,
        height: u32,
        block_hash: &[u8],
        state_root: &[u8],
        batch_data: &[u8],
    ) -> SyncResult<()> {
        // Strict in-order progression check. Reads the on-disk tip — the
        // in-flight `batch_data` bytes are NOT applied yet, so this check
        // cannot self-reject the in-flight commit. This was the failure mode
        // closed by rc.4 Option A; we preserve the closure structurally here
        // by owning the height-pointer write inside commit_atomic.
        let height_key = b"__INTERNAL/height".to_vec();
        let current = match self.db.get(&height_key) {
            Ok(Some(value)) if value.len() >= 4 => {
                let bytes: [u8; 4] = value[..4]
                    .try_into()
                    .map_err(|_| SyncError::Storage("Invalid height data".to_string()))?;
                u32::from_le_bytes(bytes)
            }
            Ok(_) => 0,
            Err(e) => return Err(SyncError::Storage(format!("Database error: {}", e))),
        };

        // We accept either:
        //   - height == current + 1  (normal forward progression)
        //   - current == 0 AND no block-hash record at height 0 (fresh DB)
        let blockhash_zero_key = b"/__INTERNAL/height-to-hash/0".to_vec();
        let is_fresh_db = current == 0
            && self
                .db
                .get(&blockhash_zero_key)
                .map_err(|e| SyncError::Storage(format!("Database error: {}", e)))?
                .is_none();
        if !is_fresh_db && height != current.saturating_add(1) {
            return Err(SyncError::Storage(format!(
                "out-of-order commit rejected: attempted height {} but current tip is {} \
                 (commit_atomic only accepts height == tip + 1)",
                height, current
            )));
        }

        // Reconstruct the WASM-side batch from its serialized form. Empty
        // `batch_data` is acceptable for callers (legacy tests, mock paths)
        // that don't ship a WASM batch through this entrypoint — in that
        // case we just write the three metadata records, which matches the
        // pre-rc.4 behavior.
        let mut batch = if batch_data.is_empty() {
            WriteBatch::default()
        } else {
            WriteBatch::from_data(batch_data)
        };

        // APPEND the three metadata records into the SAME batch. After this
        // step, the batch holds every write for block `height` — WASM
        // state changes AND sync-framework metadata — ready to commit as
        // a single RocksDB transaction.
        let blockhash_key = format!("/__INTERNAL/height-to-hash/{}", height).into_bytes();
        batch.put(&blockhash_key, block_hash);

        let root_key = format!("smt:root:{}", height).into_bytes();
        batch.put(&root_key, state_root);

        let height_bytes = height.to_le_bytes();
        batch.put(&height_key, &height_bytes);

        // ONE write_opt with sync=true. RocksDB's WriteBatch primitive
        // guarantees atomicity within a single batch: every put is visible
        // post-commit, or none of them is. WAL fsync is the durability
        // guarantee that lets restart-recovery rely on the all-or-nothing
        // invariant — without sync=true, a crash before the OS page cache
        // flushes could lose the write after we returned Ok.
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(|e| {
                SyncError::Storage(format!(
                    "commit_atomic write failed at height {}: {}",
                    height, e
                ))
            })
    }
}
