//! After `rollback_to_height(h)` both height pointers read `h` and no block
//! hash record is left above `h`, on both rollback paths.
//!
//! testnet4 rockshrew-a (v9.1.0-rc.1, 2026-10-16) and the alspo indexers
//! (2026-10-14) rolled storage back after 1-block reorgs; every restart
//! afterwards logged `heal_pointers_atomic: reconciled pointers at height
//! N-1 (was indexed=N-1, runtime_tip=N)`, because the rollback moved
//! `__INTERNAL/height` but not the runtime-side `/__INTERNAL/tip-height`
//! that every block's commit batch writes.

use metashrew_sync::StorageAdapter;
use rocksdb::WriteBatch;
use rockshrew_runtime::{RocksDBRuntimeAdapter, RocksDBStorageAdapter};
use tempfile::tempdir;

const TIP_KEY: &[u8] = b"/__INTERNAL/tip-height";

/// The per-block batch the runtime hands to `commit_atomic`: state, the
/// runtime tip pointer and (when `manifest`) the per-height key manifest
/// that selects the fast rollback path.
fn block_batch(height: u32, manifest: bool) -> Vec<u8> {
    let key = format!("some/state/{}", height).into_bytes();
    let mut b = WriteBatch::default();
    b.put(TIP_KEY, height.to_le_bytes());
    b.put(&key, b"v");
    if manifest {
        b.put(
            format!("{}{}", metashrew_runtime::smt::MANIFEST_PREFIX, height),
            metashrew_runtime::smt::serialize_key_manifest(&[key.as_slice()]),
        );
    }
    b.data().to_vec()
}

async fn rollback_case(manifest: bool) {
    let dir = tempdir().unwrap();
    let adapter =
        RocksDBRuntimeAdapter::open_optimized(dir.path().to_str().unwrap().to_string()).unwrap();
    let mut storage = RocksDBStorageAdapter::new(adapter.db.clone());

    for h in 0u32..=156_755 - 156_750 {
        let h = 156_750 + h;
        storage
            .commit_atomic(h, &[h as u8; 32], &[0u8; 32], &block_batch(h, manifest))
            .await
            .unwrap_or_else(|e| panic!("commit {}: {}", h, e));
    }
    assert_eq!(storage.get_indexed_height().await.unwrap(), 156_755);
    assert_eq!(storage.get_runtime_tip_height().await.unwrap(), 156_755);

    // 156755 orphaned: roll back to the common ancestor.
    storage.rollback_to_height(156_754).await.unwrap();
    assert_eq!(storage.get_indexed_height().await.unwrap(), 156_754);
    assert_eq!(
        storage.get_runtime_tip_height().await.unwrap(),
        156_754,
        "rollback left /__INTERNAL/tip-height ahead of __INTERNAL/height"
    );
    assert_eq!(
        storage.get_block_hash(156_755).await.unwrap(),
        None,
        "the orphaned block's hash record must go with it"
    );

    // Deeper: two more blocks.
    storage.rollback_to_height(156_752).await.unwrap();
    assert_eq!(storage.get_indexed_height().await.unwrap(), 156_752);
    assert_eq!(storage.get_runtime_tip_height().await.unwrap(), 156_752);
    assert_eq!(storage.get_block_hash(156_753).await.unwrap(), None);
    assert_eq!(storage.get_block_hash(156_754).await.unwrap(), None);
    assert!(storage.get_block_hash(156_752).await.unwrap().is_some());

    // The chain continues from the rolled-back tip.
    storage
        .commit_atomic(
            156_753,
            &[0xee; 32],
            &[0u8; 32],
            &block_batch(156_753, manifest),
        )
        .await
        .expect("re-extending the tip after rollback must be accepted");
}

#[tokio::test]
async fn rollback_moves_both_pointers_manifest_path() {
    rollback_case(true).await;
}

#[tokio::test]
async fn rollback_moves_both_pointers_full_scan_path() {
    rollback_case(false).await;
}
