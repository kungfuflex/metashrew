//! Reproduces the metashrew_preview side-effect bug.
//!
//! Background — what preview() *should* do
//! ----------------------------------------
//! `MetashrewRuntime::preview(...)` is the host-side primitive used by
//! mempool simulation, alkanes_simulate, etc. The contract is: it executes
//! the indexer against a hypothetical block N+1 and returns a view-function
//! result, **without persisting any state changes**. After the call, the
//! production DB at height N must be byte-identical to its pre-call state.
//!
//! Inside preview(), the isolation primitive is:
//!
//!     let preview_db = guard.db.create_isolated_copy();
//!
//! and then `setup_linker_preview` wires that `preview_db` to the WASM
//! __flush host fn, which writes via `ctx.db.write(batch)` — the assumption
//! being that `ctx.db` is a snapshot/scratch DB nobody else reads from.
//!
//! What's actually happening
//! -------------------------
//! `KeyValueStoreLike::create_isolated_copy()` has a default implementation
//! in `crates/metashrew-runtime/src/traits.rs` that is just `self.clone()`.
//! `RocksDBRuntimeAdapter` (the prod adapter) does **not override it**. Its
//! Clone impl is `#[derive(Clone)]`, and its `db` field is `Arc<DB>` — so
//! "cloning" just bumps the Arc refcount. Both handles point at the same
//! physical RocksDB.
//!
//! Result: every `metashrew_preview` call writes its hypothetical state
//! changes into the production DB. When the indexed block actually lands
//! later, alkane contracts that store dedup keys (e.g. `/seen/<txid>`,
//! `/tx-hashes/<txid>`, `/upgraded_seen/<height>`) see the keys already
//! exist from the preview pass and revert with
//!     `ALKANES: revert: Error: transaction already processed`
//!
//! This explains the trace divergence we observed on alkanode where
//! Subfrost (which does not run preview heavily on the same DB) accepts
//! txs that alkanode reverts.
//!
//! This test
//! ---------
//! Directly exercises the `create_isolated_copy()` trait method on the
//! RocksDB adapter and asserts the contract: a write to the "copy" must
//! not be visible from the original. **It is expected to fail today** and
//! the failure is the bug. The fix is to override `create_isolated_copy`
//! on `RocksDBRuntimeAdapter` with a real implementation (an in-memory
//! overlay that reads through to the underlying DB but routes writes to
//! a per-preview HashMap) — or to change `setup_linker_preview` so it
//! never calls into `ctx.db` for writes at all.

use metashrew_runtime::traits::KeyValueStoreLike;
use rockshrew_runtime::RocksDBRuntimeAdapter;
use tempfile::TempDir;

#[test]
fn create_isolated_copy_is_actually_isolated() {
    let dir = TempDir::new().expect("tempdir");
    let mut original =
        RocksDBRuntimeAdapter::open_optimized(dir.path().to_string_lossy().to_string())
            .expect("open RocksDBRuntimeAdapter");

    // Seed: original has key=A, value=1.
    KeyValueStoreLike::put(&mut original, b"A", b"1").expect("seed put");
    assert_eq!(
        KeyValueStoreLike::get(&mut original, b"A").expect("read seed"),
        Some(b"1".to_vec()),
        "sanity: seed write must round-trip",
    );

    // Take an "isolated copy" the same way preview() does.
    let mut preview = original.create_isolated_copy();

    // Mutate via the preview handle. Per the trait contract this
    // must NOT touch the original DB.
    KeyValueStoreLike::put(&mut preview, b"A", b"PREVIEW_MUTATION").expect("preview write");

    // The original must still report the seed value.
    let original_value =
        KeyValueStoreLike::get(&mut original, b"A").expect("read original after preview");

    assert_eq!(
        original_value,
        Some(b"1".to_vec()),
        "create_isolated_copy() returned a non-isolated handle. \
         A write through the 'preview copy' bled through to the original. \
         The 'isolated' copy is just an Arc::clone of the same RocksDB. \
         See setup_linker_preview in crates/metashrew-runtime/src/runtime.rs:1655 \
         and the default trait impl in \
         crates/metashrew-runtime/src/traits.rs:375. \
         original now reads back: {:?}",
        original_value,
    );
}

/// Same shape, but exercises the contract through the trait dispatch
/// path that `preview()` actually uses (write batch via `db.write(batch)`),
/// not just direct `put`. This is the path that bites real users because
/// alkanes-rs flushes through batched writes.
#[test]
fn create_isolated_copy_isolates_batch_writes() {
    use metashrew_runtime::traits::BatchLike;

    let dir = TempDir::new().expect("tempdir");
    let mut original =
        RocksDBRuntimeAdapter::open_optimized(dir.path().to_string_lossy().to_string())
            .expect("open RocksDBRuntimeAdapter");

    // Seed the dedup-style key alkane contracts use.
    let seen_key = b"/seen/abcdef".to_vec();
    KeyValueStoreLike::put(&mut original, &seen_key, b"v0").expect("seed");

    let mut preview = original.create_isolated_copy();

    // Simulate setup_linker_preview's path: build a batch on the preview
    // handle and commit it.
    let mut batch = preview.create_batch();
    batch.put(&seen_key, b"PREVIEW_DEDUP_VALUE");
    KeyValueStoreLike::write(&mut preview, batch).expect("preview batch write");

    let after = KeyValueStoreLike::get(&mut original, &seen_key)
        .expect("read original after preview batch");

    assert_eq!(
        after,
        Some(b"v0".to_vec()),
        "Batch write through the 'preview copy' bled through to the \
         original DB. This is the alkanes-rs failure mode: every preview \
         that calls __flush writes /seen/<txid> into prod state, and the \
         next time the real block lands the contract trips its idempotency \
         guard and reverts. original reads back: {:?}",
        after,
    );
}
