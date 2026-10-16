//! SMT Rollback Trait and Implementation
//!
//! This module provides a trait for properly rolling back Sparse Merkle Tree (SMT) data
//! during blockchain reorganizations. Both RocksDB and in-memory storage adapters must
//! implement this trait to ensure consistent reorg handling.

use crate::smt::{deserialize_key_manifest, MANIFEST_PREFIX, SMT_ROOT_PREFIX};
use anyhow::Result;
use log::{debug, info, warn};

/// One write operation queued during rollback. Used by [`SmtRollback::apply_atomic`]
/// so a backend that supports batching (e.g. RocksDB) can commit an entire height's
/// rollback in a single atomic write — preventing partial state if the process is
/// killed (OOM, SIGKILL) or the disk fills up mid-rollback.
#[derive(Clone)]
pub enum RollbackOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// Trait for rolling back SMT data during blockchain reorganizations
pub trait SmtRollback {
    /// Iterate over all keys in storage (streaming, memory-efficient)
    fn iter_keys<F>(&self, callback: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>;

    /// Delete a key from storage
    fn delete_key(&mut self, key: &[u8]) -> Result<()>;

    /// Put a key-value pair into storage
    fn put_key(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Get value for a key
    fn get_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Apply a list of ops as a single atomic write. The default falls through
    /// to individual `put_key` / `delete_key` calls and is therefore NOT atomic;
    /// RocksDB and other backends with native batching MUST override this.
    fn apply_atomic(&mut self, ops: &[RollbackOp]) -> Result<()> {
        for op in ops {
            match op {
                RollbackOp::Put(k, v) => self.put_key(k, v)?,
                RollbackOp::Delete(k) => self.delete_key(k)?,
            }
        }
        Ok(())
    }
}

/// Parse height from an SMT update key
///
/// SMT keys are stored as: `base_key/index`
/// SMT values are stored as: `height:data`
///
/// This function extracts the height from the value.
fn parse_height_from_smt_value(value: &[u8]) -> Option<u32> {
    let value_str = String::from_utf8_lossy(value);
    if let Some(colon_pos) = value_str.find(':') {
        let height_str = &value_str[..colon_pos];
        height_str.parse::<u32>().ok()
    } else {
        None
    }
}

/// Roll back SMT data to a specific height
///
/// This is the correct implementation that BOTH RocksDB and MemStore should use.
///
/// The rollback process:
/// 1. Delete metadata keys (block_hash_*, state_root_*, smt:root:*, /__INTERNAL/height-to-hash/*) for heights > rollback_height
/// 2. Roll back append-only SMT data structures (keys with /length suffix)
/// 3. This ensures all WASM-indexed data is properly cleaned up during reorgs
///
/// Memory profile: collects all `/length` base-key paths in memory before
/// processing. The previous implementation silently truncated this list at
/// 100k entries, which produced an incomplete (and therefore divergent)
/// rollback on any database with more than 100k tracked keys. The cap is
/// removed; on extremely large databases the operator may briefly see high
/// RSS during reorg, but the database stays consistent. The fast manifest
/// path (`rollback_with_manifests`) is preferred for any non-genesis reorg.
pub fn rollback_smt_data<S: SmtRollback>(
    storage: &mut S,
    rollback_height: u32,
    current_height: u32,
) -> Result<()> {
    info!(
        "Starting SMT rollback from height {} to height {}",
        current_height, rollback_height
    );

    if rollback_height >= current_height {
        debug!(
            "Rollback height {} >= current height {}, nothing to do",
            rollback_height, current_height
        );
        return Ok(());
    }

    // --- Step 1: Single scan — collect metadata keys to delete and SMT base keys.
    let length_suffix = b"/length";
    let mut metadata_keys_to_delete = Vec::new();
    let mut base_keys: Vec<Vec<u8>> = Vec::new();

    storage.iter_keys(|key| {
        let key_str = String::from_utf8_lossy(key);

        let metadata_height = if let Some(stripped) = key_str.strip_prefix("block_hash_") {
            stripped.parse::<u32>().ok()
        } else if let Some(stripped) = key_str.strip_prefix("state_root_") {
            stripped.parse::<u32>().ok()
        } else if let Some(stripped) = key_str.strip_prefix("smt:root:") {
            stripped.parse::<u32>().ok()
        } else if let Some(stripped) = key_str.strip_prefix("/__INTERNAL/height-to-hash/") {
            // Block-hash records written by `commit_atomic`. The manifest
            // path deletes them; without this the full-scan path left the
            // dead branch's hashes above the rollback height.
            stripped.parse::<u32>().ok()
        } else {
            None
        };

        if let Some(h) = metadata_height {
            if h > rollback_height {
                metadata_keys_to_delete.push(key.to_vec());
            }
        } else if key.ends_with(length_suffix) {
            let base_key = &key[..key.len() - length_suffix.len()];
            base_keys.push(base_key.to_vec());
        }

        Ok(())
    })?;

    // Atomic delete of stale metadata records.
    {
        let ops: Vec<RollbackOp> = metadata_keys_to_delete
            .iter()
            .map(|k| RollbackOp::Delete(k.clone()))
            .collect();
        storage.apply_atomic(&ops)?;
        info!(
            "Deleted {} metadata keys atomically",
            metadata_keys_to_delete.len()
        );
    }

    base_keys.sort_unstable();
    base_keys.dedup();
    info!(
        "Collected {} unique SMT structures to process",
        base_keys.len()
    );

    // Each SMT structure's read/modify/write ops commit atomically per
    // structure. A crash between structures leaves earlier ones rolled back,
    // later ones intact — all idempotent on retry.
    let mut smt_structures_rolled_back = 0;

    for base_key in &base_keys {
        let mut length_key = base_key.clone();
        length_key.extend_from_slice(length_suffix);

        let old_length = if let Some(length_bytes) = storage.get_value(&length_key)? {
            String::from_utf8_lossy(&length_bytes)
                .parse::<u32>()
                .unwrap_or(0)
        } else {
            continue;
        };

        // Collect surviving updates (those at or below rollback_height).
        let mut valid_updates = Vec::new();
        for i in 0..old_length {
            let update_key_suffix = format!("/{}", i);
            let mut update_key = base_key.clone();
            update_key.extend_from_slice(update_key_suffix.as_bytes());

            if let Some(update_data) = storage.get_value(&update_key)? {
                if let Some(update_height) = parse_height_from_smt_value(&update_data) {
                    if update_height <= rollback_height {
                        valid_updates.push((i, update_data));
                    } else {
                        debug!(
                            "Removing SMT update at height {} (> {})",
                            update_height, rollback_height
                        );
                    }
                }
            }
        }

        // Build a single atomic batch per structure: delete every old slot,
        // re-insert the survivors at compacted indices, update or remove the
        // /length entry.
        let mut ops: Vec<RollbackOp> = Vec::with_capacity(old_length as usize + 1);
        for i in 0..old_length {
            let update_key_suffix = format!("/{}", i);
            let mut update_key = base_key.clone();
            update_key.extend_from_slice(update_key_suffix.as_bytes());
            ops.push(RollbackOp::Delete(update_key));
        }
        for (new_index, (_, update_data)) in valid_updates.iter().enumerate() {
            let update_key_suffix = format!("/{}", new_index);
            let mut update_key = base_key.clone();
            update_key.extend_from_slice(update_key_suffix.as_bytes());
            ops.push(RollbackOp::Put(update_key, update_data.clone()));
        }
        let new_length = valid_updates.len() as u32;
        if new_length > 0 {
            ops.push(RollbackOp::Put(
                length_key.clone(),
                new_length.to_string().into_bytes(),
            ));
            debug!(
                "SMT structure {} compacted from {} to {} entries",
                String::from_utf8_lossy(base_key),
                old_length,
                new_length
            );
        } else {
            ops.push(RollbackOp::Delete(length_key.clone()));
            debug!(
                "SMT structure {} completely removed (no valid entries)",
                String::from_utf8_lossy(base_key)
            );
        }

        storage.apply_atomic(&ops)?;
        smt_structures_rolled_back += 1;

        if smt_structures_rolled_back % 1000 == 0 {
            info!(
                "Rolled back {} SMT structures so far...",
                smt_structures_rolled_back
            );
        }
    }

    info!(
        "Successfully rolled back {} SMT data structures to height {}",
        smt_structures_rolled_back, rollback_height
    );
    Ok(())
}

/// Fast rollback using per-height manifests.
///
/// Instead of scanning all keys in the database, reads the manifest for each
/// rolled-back height to find exactly which keys were modified, then rolls
/// back only those keys. Falls back to `rollback_smt_data()` if manifests
/// are missing (e.g., blocks indexed before the upgrade).
///
/// Returns Ok(true) if fast rollback succeeded, Ok(false) if fallback is needed.
pub fn rollback_with_manifests<S: SmtRollback>(
    storage: &mut S,
    rollback_height: u32,
    current_height: u32,
) -> Result<bool> {
    if rollback_height >= current_height {
        return Ok(true);
    }

    info!(
        "Attempting fast manifest-based rollback from height {} to {}",
        current_height, rollback_height
    );

    // Check if manifests exist for all heights in the rollback range
    for h in (rollback_height + 1)..=current_height {
        let manifest_key = format!("{}{}", MANIFEST_PREFIX, h).into_bytes();
        if storage.get_value(&manifest_key)?.is_none() {
            warn!(
                "Manifest missing for height {}. Falling back to full rollback.",
                h
            );
            return Ok(false);
        }
    }

    // All manifests exist — do the fast rollback. Each height's rollback is
    // committed as a single atomic batch (`apply_atomic`) so an OOM kill or
    // ENOSPC interrupting the loop leaves the database at a clean
    // intermediate height — never with a half-trimmed append-only chain.
    let mut total_keys_rolled_back = 0;

    for h in ((rollback_height + 1)..=current_height).rev() {
        let manifest_key = format!("{}{}", MANIFEST_PREFIX, h).into_bytes();
        let manifest_data = storage.get_value(&manifest_key)?.unwrap();
        let keys = deserialize_key_manifest(&manifest_data);

        let mut ops: Vec<RollbackOp> = Vec::new();

        for key in &keys {
            // For each key modified at this height, trim append-only entries
            // above the rollback height
            let length_key = [key.as_slice(), b"/length"].concat();
            if let Some(length_bytes) = storage.get_value(&length_key)? {
                let length = String::from_utf8_lossy(&length_bytes)
                    .parse::<u32>()
                    .unwrap_or(0);

                // Walk backward from the end to find entries above rollback_height
                let mut new_length = length;
                for i in (0..length).rev() {
                    let update_key = [key.as_slice(), b"/", i.to_string().as_bytes()].concat();
                    if let Some(update_data) = storage.get_value(&update_key)? {
                        if let Some(entry_height) = parse_height_from_smt_value(&update_data) {
                            if entry_height > rollback_height {
                                ops.push(RollbackOp::Delete(update_key));
                                new_length = i;
                            } else {
                                break; // entries are ordered by height, stop early
                            }
                        }
                    }
                }

                if new_length != length {
                    if new_length > 0 {
                        ops.push(RollbackOp::Put(
                            length_key.clone(),
                            new_length.to_string().into_bytes(),
                        ));
                    } else {
                        ops.push(RollbackOp::Delete(length_key.clone()));
                    }
                }
            }
            total_keys_rolled_back += 1;
        }

        // Manifest and metadata records for this height go into the same
        // atomic batch — so the manifest is never gone until the trims are
        // also committed.
        ops.push(RollbackOp::Delete(manifest_key));
        ops.push(RollbackOp::Delete(
            format!("{}{}", SMT_ROOT_PREFIX, h).into_bytes(),
        ));
        ops.push(RollbackOp::Delete(format!("block_hash_{}", h).into_bytes()));
        ops.push(RollbackOp::Delete(format!("state_root_{}", h).into_bytes()));
        // Sync-framework records written by `__flush` (atomic block-commit
        // path) live under different key names — clear them too.
        ops.push(RollbackOp::Delete(
            format!("/__INTERNAL/height-to-hash/{}", h).into_bytes(),
        ));

        storage.apply_atomic(&ops)?;
    }

    info!(
        "Fast rollback complete: rolled back {} keys across {} heights",
        total_keys_rolled_back,
        current_height - rollback_height
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockStorage {
        data: HashMap<Vec<u8>, Vec<u8>>,
    }

    impl SmtRollback for MockStorage {
        fn iter_keys<F>(&self, mut callback: F) -> Result<()>
        where
            F: FnMut(&[u8]) -> Result<()>,
        {
            for key in self.data.keys() {
                callback(key)?;
            }
            Ok(())
        }

        fn delete_key(&mut self, key: &[u8]) -> Result<()> {
            self.data.remove(key);
            Ok(())
        }

        fn put_key(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn get_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.data.get(key).cloned())
        }
    }

    #[test]
    fn test_rollback_metadata_keys() {
        let mut storage = MockStorage {
            data: HashMap::new(),
        };

        // Insert metadata for heights 3, 4, 5
        storage
            .data
            .insert(b"block_hash_3".to_vec(), b"hash3".to_vec());
        storage
            .data
            .insert(b"block_hash_4".to_vec(), b"hash4".to_vec());
        storage
            .data
            .insert(b"block_hash_5".to_vec(), b"hash5".to_vec());
        storage
            .data
            .insert(b"smt:root:3".to_vec(), b"root3".to_vec());
        storage
            .data
            .insert(b"smt:root:4".to_vec(), b"root4".to_vec());
        storage
            .data
            .insert(b"smt:root:5".to_vec(), b"root5".to_vec());

        // Rollback to height 3
        rollback_smt_data(&mut storage, 3, 5).unwrap();

        // Heights 4 and 5 should be deleted, height 3 should remain
        assert!(storage.data.contains_key(b"block_hash_3".as_ref()));
        assert!(!storage.data.contains_key(b"block_hash_4".as_ref()));
        assert!(!storage.data.contains_key(b"block_hash_5".as_ref()));
        assert!(storage.data.contains_key(b"smt:root:3".as_ref()));
        assert!(!storage.data.contains_key(b"smt:root:4".as_ref()));
        assert!(!storage.data.contains_key(b"smt:root:5".as_ref()));
    }

    #[test]
    fn test_parse_height_from_smt_value() {
        assert_eq!(parse_height_from_smt_value(b"123:data"), Some(123));
        assert_eq!(parse_height_from_smt_value(b"0:data"), Some(0));
        assert_eq!(parse_height_from_smt_value(b"noheight"), None);
        assert_eq!(parse_height_from_smt_value(b":data"), None);
    }

    #[test]
    fn test_manifest_rollback_basic() {
        use crate::smt::{serialize_key_manifest, MANIFEST_PREFIX, SMT_ROOT_PREFIX};

        let mut storage = MockStorage {
            data: HashMap::new(),
        };

        // Simulate indexing 3 blocks (heights 1, 2, 3)
        // Block 1: modifies key_a, key_b
        storage.data.insert(b"key_a/length".to_vec(), b"1".to_vec());
        storage.data.insert(b"key_a/0".to_vec(), b"1:aa".to_vec());
        storage.data.insert(b"key_b/length".to_vec(), b"1".to_vec());
        storage.data.insert(b"key_b/0".to_vec(), b"1:bb".to_vec());
        let manifest1 = serialize_key_manifest(&[b"key_a", b"key_b"]);
        storage
            .data
            .insert(format!("{}1", MANIFEST_PREFIX).into_bytes(), manifest1);
        storage.data.insert(
            format!("{}1", SMT_ROOT_PREFIX).into_bytes(),
            b"root1".to_vec(),
        );
        storage
            .data
            .insert(b"block_hash_1".to_vec(), b"hash1".to_vec());

        // Block 2: modifies key_a, key_c
        storage.data.insert(b"key_a/length".to_vec(), b"2".to_vec());
        storage.data.insert(b"key_a/1".to_vec(), b"2:aa2".to_vec());
        storage.data.insert(b"key_c/length".to_vec(), b"1".to_vec());
        storage.data.insert(b"key_c/0".to_vec(), b"2:cc".to_vec());
        let manifest2 = serialize_key_manifest(&[b"key_a", b"key_c"]);
        storage
            .data
            .insert(format!("{}2", MANIFEST_PREFIX).into_bytes(), manifest2);
        storage.data.insert(
            format!("{}2", SMT_ROOT_PREFIX).into_bytes(),
            b"root2".to_vec(),
        );
        storage
            .data
            .insert(b"block_hash_2".to_vec(), b"hash2".to_vec());

        // Block 3: modifies key_b
        storage.data.insert(b"key_b/length".to_vec(), b"2".to_vec());
        storage.data.insert(b"key_b/1".to_vec(), b"3:bb3".to_vec());
        let manifest3 = serialize_key_manifest(&[b"key_b"]);
        storage
            .data
            .insert(format!("{}3", MANIFEST_PREFIX).into_bytes(), manifest3);
        storage.data.insert(
            format!("{}3", SMT_ROOT_PREFIX).into_bytes(),
            b"root3".to_vec(),
        );
        storage
            .data
            .insert(b"block_hash_3".to_vec(), b"hash3".to_vec());

        // Rollback to height 1 (undo blocks 2 and 3)
        let result = rollback_with_manifests(&mut storage, 1, 3).unwrap();
        assert!(result, "fast rollback should succeed with manifests");

        // key_a should be back to length 1 (only block 1 entry)
        assert_eq!(
            storage.data.get(b"key_a/length".as_ref()),
            Some(&b"1".to_vec())
        );
        assert!(storage.data.contains_key(b"key_a/0".as_ref())); // block 1 entry kept
        assert!(!storage.data.contains_key(b"key_a/1".as_ref())); // block 2 entry removed

        // key_b should be back to length 1 (block 3 entry removed)
        assert_eq!(
            storage.data.get(b"key_b/length".as_ref()),
            Some(&b"1".to_vec())
        );
        assert!(storage.data.contains_key(b"key_b/0".as_ref())); // block 1 entry kept
        assert!(!storage.data.contains_key(b"key_b/1".as_ref())); // block 3 entry removed

        // key_c should be completely removed (only existed from block 2)
        assert!(!storage.data.contains_key(b"key_c/length".as_ref()));
        assert!(!storage.data.contains_key(b"key_c/0".as_ref()));

        // Metadata for heights 2 and 3 should be gone
        assert!(!storage
            .data
            .contains_key(format!("{}2", SMT_ROOT_PREFIX).as_bytes()));
        assert!(!storage
            .data
            .contains_key(format!("{}3", SMT_ROOT_PREFIX).as_bytes()));
        assert!(!storage.data.contains_key(b"block_hash_2".as_ref()));
        assert!(!storage.data.contains_key(b"block_hash_3".as_ref()));

        // Height 1 metadata should remain
        assert!(storage
            .data
            .contains_key(format!("{}1", SMT_ROOT_PREFIX).as_bytes()));
        assert!(storage.data.contains_key(b"block_hash_1".as_ref()));

        // Manifests for 2 and 3 should be deleted
        assert!(!storage
            .data
            .contains_key(format!("{}2", MANIFEST_PREFIX).as_bytes()));
        assert!(!storage
            .data
            .contains_key(format!("{}3", MANIFEST_PREFIX).as_bytes()));
    }

    #[test]
    fn test_manifest_rollback_falls_back_without_manifests() {
        let mut storage = MockStorage {
            data: HashMap::new(),
        };

        // No manifests — should return false for fallback
        storage
            .data
            .insert(b"block_hash_2".to_vec(), b"hash2".to_vec());
        let result = rollback_with_manifests(&mut storage, 1, 2).unwrap();
        assert!(!result, "should fall back when manifests are missing");
    }
}
