//! Regression test for the 2026-10-14 alspo pointer divergence.
//!
//! A 1-block mainnet reorg at 971345 made the running indexer roll storage
//! back to 971344 via `RocksDBStorageAdapter::rollback_to_height`. That
//! moved `__INTERNAL/height` but left the runtime-side
//! `/__INTERNAL/tip-height` (written into every block's commit batch) at
//! 971345. The next restart logged:
//!
//!   heal_pointers_atomic: reconciled pointers at height 971344
//!   (was indexed=971344, runtime_tip=971345)
//!
//! The invariant pinned here: after `rollback_to_height(h)` BOTH pointers
//! read `h`, so the startup heal has nothing to reconcile.

use metashrew_sync::StorageAdapter;
use rockshrew_runtime::{RocksDBRuntimeAdapter, RocksDBStorageAdapter};
use rocksdb::WriteBatch;
use tempfile::tempdir;

const TIP_KEY: &[u8] = b"/__INTERNAL/tip-height";

/// The runtime puts the tip pointer into the per-block batch it hands to
/// `commit_atomic` (metashrew-runtime smt.rs build_state_root_batch_unwritten).
fn block_batch(height: u32) -> Vec<u8> {
    let mut b = WriteBatch::default();
    b.put(TIP_KEY, height.to_le_bytes());
    b.put(format!("some/state/{}", height).as_bytes(), b"v");
    b.data().to_vec()
}

#[tokio::test]
async fn rollback_moves_runtime_tip_pointer_with_indexed_height() {
    let dir = tempdir().unwrap();
    let adapter =
        RocksDBRuntimeAdapter::open_optimized(dir.path().to_str().unwrap().to_string()).unwrap();
    let mut storage = RocksDBStorageAdapter::new(adapter.db.clone());

    for h in 971_343u32..=971_345 {
        storage
            .commit_atomic(h, &[h as u8; 32], &[0u8; 32], &block_batch(h))
            .await
            .unwrap();
    }
    assert_eq!(storage.get_indexed_height().await.unwrap(), 971_345);
    assert_eq!(storage.get_runtime_tip_height().await.unwrap(), 971_345);

    // The reorg: 971345 was orphaned, roll back to the common ancestor.
    storage.rollback_to_height(971_344).await.unwrap();

    assert_eq!(storage.get_indexed_height().await.unwrap(), 971_344);
    assert_eq!(
        storage.get_runtime_tip_height().await.unwrap(),
        971_344,
        "rollback left /__INTERNAL/tip-height ahead of __INTERNAL/height \
         (the indexed=971344 runtime_tip=971345 divergence)"
    );

    // And the chain continues from the rolled-back tip.
    storage
        .commit_atomic(971_345, &[0xee; 32], &[0u8; 32], &block_batch(971_345))
        .await
        .expect("re-extending the tip after rollback must be accepted");
}
