//! Exhaustive reorg / chain-tip-mutation regression suite.
//!
//! These tests prove that a re-organisation correctly resets per-outpoint
//! and per-tx state in the WASM-indexed view. The framing matches the
//! `protorunesbyoutpoint` + `alkanes_trace` failure mode that bit the
//! Subfrost mobile wallet (wrap tx 087d8cbf01af… at block 949241 with
//! empty alkane state at vout 1 despite a canonical on-chain confirmation).
//!
//! Each test below corresponds to one of the ten scenarios listed in the
//! deep-dive prompt. The scenarios live here so they can grow over time
//! without touching the older `reorg_focused_test.rs` / `reorg_rollback_bug_test.rs`
//! files, which exist primarily to gate the SMT-rollback fix that landed in
//! `REORG_ROLLBACK_FIX.md`.
//!
//! Per-test pattern:
//!   1. Build chain A with a specific set of protostone-bearing transactions
//!   2. Sync chain A through `MetashrewSync`
//!   3. Assert the per-outpoint / per-tx state matches chain A
//!   4. Swap the mock node to chain B (forking at some height)
//!   5. Sync chain B (this triggers `handle_reorg`, which fires
//!      `rollback_to_height` + `refresh_memory`)
//!   6. Assert the per-outpoint / per-tx state now matches chain B (and is
//!      zero where chain B did not touch the outpoint)
//!
//! The protostone wire format and the opcodes are defined in
//! `block_builder.rs` and parsed by `metashrew-minimal`'s `_start`. Three
//! opcodes are enough to model the failure modes that matter:
//!   - MINT     creates per-outpoint state at vout 1
//!   - BURN     zeroes per-outpoint state at vout 1
//!   - TRANSFER zeroes the input outpoint, sets vout 1 of this tx
//!
//! Combining MINT and TRANSFER models the wrap+swap CPFP bundle (the
//! parent reorg test).
//!
//! Failures here fail loud: every assertion compares the SMT-extracted
//! value byte-for-byte against the expected chain-B truth, so any
//! residual chain-A data appears in the panic message immediately.

use crate::block_builder::{
    build_protostone_tx, BlockBuilder, ChainBuilder, PROTOSTONE_OP_BURN, PROTOSTONE_OP_MINT,
    PROTOSTONE_OP_TRANSFER,
};
use crate::TestConfig;
use anyhow::Result;
use async_trait::async_trait;
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, Transaction, Txid};
use log::info;
use memshrew_runtime::MemStoreAdapter;
use metashrew_runtime::smt::SMTHelper;
use metashrew_sync::{
    adapters::MetashrewRuntimeAdapter, BitcoinNodeAdapter, BlockInfo, ChainTip, SyncConfig,
    SyncEngine, SyncResult,
};
use std::sync::Arc;
use tokio::sync::Mutex;

// -----------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------

/// MockNode shared by every test. Mirrors the implementation in
/// `reorg_focused_test.rs` and `comprehensive_e2e_test.rs` — duplicated
/// rather than extracted to avoid touching those test files.
#[derive(Clone)]
struct MockNode {
    chain: Arc<Mutex<ChainBuilder>>,
}

impl MockNode {
    fn new(chain: ChainBuilder) -> Self {
        Self {
            chain: Arc::new(Mutex::new(chain)),
        }
    }

    async fn switch_chain(&self, new_chain: ChainBuilder) {
        let mut chain = self.chain.lock().await;
        *chain = new_chain;
    }
}

#[async_trait]
impl BitcoinNodeAdapter for MockNode {
    async fn get_tip_height(&self) -> SyncResult<u32> {
        let chain = self.chain.lock().await;
        Ok(chain.height())
    }

    async fn get_block_hash(&self, height: u32) -> SyncResult<Vec<u8>> {
        let chain = self.chain.lock().await;
        Ok(chain
            .get_block(height)
            .map(|b| {
                let mut hash = b.block_hash().to_byte_array().to_vec();
                hash.reverse();
                hash
            })
            .unwrap_or_else(|| {
                let mut hash = BlockHash::all_zeros().to_byte_array().to_vec();
                hash.reverse();
                hash
            }))
    }

    async fn get_block_data(&self, height: u32) -> SyncResult<Vec<u8>> {
        let chain = self.chain.lock().await;
        let block = chain.get_block(height).unwrap();
        Ok(metashrew_support::utils::consensus_encode(block)?)
    }

    async fn get_block_info(&self, height: u32) -> SyncResult<BlockInfo> {
        let hash = self.get_block_hash(height).await?;
        let data = self.get_block_data(height).await?;
        Ok(BlockInfo { height, hash, data })
    }

    async fn get_chain_tip(&self) -> SyncResult<ChainTip> {
        let chain = self.chain.lock().await;
        let mut hash = chain.tip_hash().to_byte_array().to_vec();
        hash.reverse();
        Ok(ChainTip {
            height: chain.height(),
            hash,
        })
    }

    async fn is_connected(&self) -> bool {
        true
    }
}

/// Sync a chain into the shared storage, exiting once the tip is indexed.
///
/// `exit_at` is the target indexed height plus one (this matches the
/// SyncConfig.exit_at semantics: stop when current_height >= exit_at).
/// Each invocation builds a fresh runtime and a fresh syncer — between
/// invocations the runtime is dropped, so the next sync only sees the
/// persisted storage and must rediscover the tip via reorg logic.
async fn sync_chain(
    config: &TestConfig,
    storage: MemStoreAdapter,
    node: MockNode,
    exit_at: u32,
) -> Result<()> {
    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = config
        .create_runtime_from_adapter(storage.clone(), engine)
        .await?;
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    let sync_config = SyncConfig {
        start_block: 0,
        exit_at: Some(exit_at),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
    };

    let mut syncer =
        metashrew_sync::sync::MetashrewSync::new(node, storage, runtime_adapter, sync_config);

    // Critical: `init()` reads the storage's `indexed_height` and seeds the
    // engine's atomic current_height to `indexed_height + 1`. Without this,
    // every call to sync_chain() starts at h=0 and just OVERWRITES the
    // existing chain (no reorg ever fires) because validate_block_connects
    // is happy with genesis matching genesis. This matches how rockshrew-mono
    // bootstraps in production via `sync_engine.init().await` in `run()`.
    syncer.init().await;
    syncer.start().await?;
    Ok(())
}

/// Read the alkane-id bytes the WASM stored at `(txid, vout)` as of the
/// given snapshot height. Matches the format written by `metashrew-minimal`'s
/// `_start` (`/outpoint/{hex(txid)}:{vout}`).
fn get_outpoint(
    adapter: &MemStoreAdapter,
    txid: Txid,
    vout: u32,
    at_height: u32,
) -> Result<Option<Vec<u8>>> {
    let smt = SMTHelper::new(adapter.clone());
    let key = format!("/outpoint/{}:{}", hex::encode(txid.as_byte_array()), vout).into_bytes();
    smt.get_at_height(&key, at_height)
}

/// Read the trace bytes the WASM stored for `txid` as of the given height.
fn get_trace(adapter: &MemStoreAdapter, txid: Txid, at_height: u32) -> Result<Option<Vec<u8>>> {
    let smt = SMTHelper::new(adapter.clone());
    let key = format!("/trace/{}", hex::encode(txid.as_byte_array())).into_bytes();
    smt.get_at_height(&key, at_height)
}

/// Read the ordered txid list for a given height as the WASM saw it.
fn get_txids_at_height(
    adapter: &MemStoreAdapter,
    height: u32,
    snapshot_height: u32,
) -> Result<Option<Vec<u8>>> {
    let smt = SMTHelper::new(adapter.clone());
    let key = format!("/txids-at-height/{}", height).into_bytes();
    smt.get_at_height(&key, snapshot_height)
}

/// Convenience: build a chain by walking a list of `(height -> txs)` callbacks.
/// The first height is always 1 (genesis has no protostones). `salt` differs
/// the coinbase scriptSig — without that, two chain builders with the same tx
/// set produce identical blocks at every height and no reorg fires. The salt
/// also rotates the coinbase value to push the merkle root around, which is
/// what makes the per-height block-hash diverge across chain A and chain B.
fn build_chain_with_txs<F>(salt: u32, num_blocks: u32, mut tx_builder: F) -> ChainBuilder
where
    F: FnMut(u32) -> Vec<Transaction>,
{
    let mut chain = ChainBuilder::new();
    for h in 1..=num_blocks {
        let txs_for_block = tx_builder(h);
        let salt_copy = salt;
        chain = chain.add_custom_block(move |builder| {
            // Bake the salt into both the coinbase scriptSig (varies the tx
            // bytes themselves) and the coinbase value (varies the output
            // value, so even a "no-extra-txs" block diverges between chains).
            let mut b = builder.add_coinbase(
                5_000_000_000u64 - salt_copy as u64 - h as u64,
                Some(&format!("salt{:08x}{:08x}", salt_copy, h)),
            );
            for tx in txs_for_block {
                b = b.add_transaction(tx);
            }
            b
        });
    }
    chain
}

/// Junk parent-outpoint used as `vin[0]` for protostone txs. The actual
/// txid is never validated by the test WASM — only the cellpack matters —
/// so we pick a stable nonce so the produced txid is deterministic across
/// chains A and B (otherwise reorg-replay tests would be checking byte
/// equality on different txids).
fn fake_parent_outpoint(nonce: u64) -> (Txid, u32) {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&nonce.to_le_bytes());
    (Txid::from_byte_array(bytes), 0)
}

// -----------------------------------------------------------------------
// Scenario 1 — Single-tx reorg.
// Tx X at block N1 -> reorg -> tx X at block N2 (same txid, different block).
// State at outpoints(X) must reflect N2 execution, not stale N1.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_1_single_tx_reorg_same_txid_different_block() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 1: Same tx X moves from block N1 to block N2");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, parent_vout) = fake_parent_outpoint(1);
    // Build tx X — pinning its inputs/outputs so the same builder produces
    // the same txid in both chains.
    let tx_x = build_protostone_tx(parent, parent_vout, 0xABCD, PROTOSTONE_OP_MINT, 0xDEADBEEF);
    let txid_x = tx_x.compute_txid();

    // Chain A: tx X at height 1, then 4 empty blocks (tip = height 5).
    let tx_x_for_a = tx_x.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 1 {
            vec![tx_x_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    // After A: outpoint(x, 1) holds the alkane, trace is non-empty.
    let outp_a = get_outpoint(&storage, txid_x, 1, 5)?;
    assert_eq!(
        outp_a.as_deref(),
        Some(0xDEADBEEFu64.to_le_bytes().as_slice()),
        "chain A: outpoint must hold the minted alkane"
    );
    let trace_a = get_trace(&storage, txid_x, 5)?;
    assert!(trace_a.is_some() && !trace_a.as_ref().unwrap().is_empty());

    // Chain B: same coinbase salt at heights 1-2 (no protostone) and tx X at
    // height 3, with a fresh coinbase salt to force a divergent block hash.
    let tx_x_for_b = tx_x.clone();
    let chain_b = build_chain_with_txs(2, 5, move |h| {
        if h == 3 {
            vec![tx_x_for_b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 6).await?;

    // Outpoint state must still be present — chain B re-applied the MINT at
    // height 3 — but the snapshot view at height 2 must be EMPTY (the tx
    // does not exist at height 2 in chain B).
    let outp_b_at_5 = get_outpoint(&storage, txid_x, 1, 5)?;
    assert_eq!(
        outp_b_at_5.as_deref(),
        Some(0xDEADBEEFu64.to_le_bytes().as_slice()),
        "chain B: outpoint must be re-minted at the new height"
    );
    let outp_b_at_2 = get_outpoint(&storage, txid_x, 1, 2)?;
    assert!(
        outp_b_at_2.is_none() || outp_b_at_2.as_deref() == Some(&[][..]),
        "chain B: at height 2 the MINT has not happened yet (was at h=1 in A); got: {:?}",
        outp_b_at_2
    );

    info!("✅ Scenario 1 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 2 — Tx-vanishes reorg.
// Tx X at N1 -> reorg -> tx X NEVER re-mined. State at outpoints(X) must
// be ZEROED (rolled back, no residual).
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_2_tx_vanishes_in_reorg() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 2: tx X mined in A, never re-mined in B — state must zero out");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, parent_vout) = fake_parent_outpoint(2);
    let tx_x = build_protostone_tx(parent, parent_vout, 0x1111, PROTOSTONE_OP_MINT, 0xCAFEBABE);
    let txid_x = tx_x.compute_txid();

    let tx_x_for_a = tx_x.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx_x_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;
    let outp_a = get_outpoint(&storage, txid_x, 1, 5)?;
    assert_eq!(
        outp_a.as_deref(),
        Some(0xCAFEBABEu64.to_le_bytes().as_slice())
    );

    // Chain B forks from height 1: same height 1, but height 2 onward is
    // empty (tx X never re-mines). Salt + 6 blocks so chain B is strictly
    // longer and wins.
    let chain_b = build_chain_with_txs(7, 6, move |_h| vec![]);
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // The historical-view query at the tip should show NO state for txid_x —
    // either explicitly None or the empty-marker that get-with-trim would
    // produce after rollback.
    let outp_b = get_outpoint(&storage, txid_x, 1, 6)?;
    let trace_b = get_trace(&storage, txid_x, 6)?;
    assert!(
        outp_b.is_none() || outp_b.as_deref() == Some(&[][..]),
        "outpoint must be zeroed after reorg; got: {:?}",
        outp_b
    );
    assert!(
        trace_b.is_none() || trace_b.as_deref() == Some(&[][..]),
        "trace must be zeroed after reorg; got: {:?}",
        trace_b
    );

    info!("✅ Scenario 2 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 3 — Different-effects reorg.
// Tx X cellpack C1 (MINT) at N1 -> reorg -> tx X cellpack C2 (BURN) at N2.
// State must reflect C2, not the C1 mint.
//
// Tx X is identified by its full transaction bytes, so to change the
// cellpack we must change the script_pubkey, which changes the txid. To
// satisfy "same txid, different effect" we cheat: tx_a and tx_b spend
// from the same parent outpoint with the SAME sequence_nonce but emit
// different protostone payloads at vout[0]. The txids differ — but the
// EFFECT differs at a per-outpoint level, which is what the test
// actually wants to prove (the wallet's fingerprint isn't about same
// txid; it's about "did the new chain's execution leave the right state
// at the canonical outpoint").
//
// For the strict same-txid form, see scenario 10 (the "already-processed
// guard" test), which uses an identical tx and proves WASM re-execution.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_3_different_effects_reorg() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 3: cellpack flips from MINT to BURN across reorg");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, parent_vout) = fake_parent_outpoint(3);

    // Variant A: MINT
    let tx_mint = build_protostone_tx(parent, parent_vout, 0x5555, PROTOSTONE_OP_MINT, 0xAA00BB11);
    let txid_mint = tx_mint.compute_txid();

    let tx_mint_clone = tx_mint.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx_mint_clone.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    let trace_a = get_trace(&storage, txid_mint, 5)?;
    // Trace starts with the opcode byte — MINT = 0x01
    assert!(trace_a.as_ref().map(|t| t[0]) == Some(0x01));

    // Variant B: BURN (same parent, same sequence_nonce, BURN opcode -> the
    // OP_RETURN payload differs by one byte, txid differs accordingly).
    let tx_burn = build_protostone_tx(parent, parent_vout, 0x5555, PROTOSTONE_OP_BURN, 0xAA00BB11);
    let txid_burn = tx_burn.compute_txid();
    assert_ne!(
        txid_mint, txid_burn,
        "MINT and BURN differ in OP_RETURN payload — txids must differ"
    );

    let tx_burn_clone = tx_burn.clone();
    let chain_b = build_chain_with_txs(9, 6, move |h| {
        if h == 2 {
            vec![tx_burn_clone.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // The MINT's outpoint state must be GONE (chain A's tx is no longer mined).
    let mint_outp_after = get_outpoint(&storage, txid_mint, 1, 6)?;
    assert!(
        mint_outp_after.is_none() || mint_outp_after.as_deref() == Some(&[][..]),
        "MINT outpoint must be zeroed after reorg; got: {:?}",
        mint_outp_after
    );

    // BURN's outpoint state must be EMPTY (BURN explicitly clears vout 1).
    let burn_outp = get_outpoint(&storage, txid_burn, 1, 6)?;
    assert_eq!(
        burn_outp.as_deref(),
        Some(&[][..]),
        "BURN must have set vout 1 to empty"
    );
    let burn_trace = get_trace(&storage, txid_burn, 6)?;
    assert!(burn_trace.as_ref().map(|t| t[0]) == Some(0x02));

    info!("✅ Scenario 3 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 4 — Multi-tx reorg with different replay order.
// N orphaned txs at block N1 all touch the same outpoint chain. Reorg
// replays them in a DIFFERENT order at N2. State must be path-correct.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_4_multi_tx_reorg_different_replay_order() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 4: three txs replayed in flipped order across reorg");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    // Three independent MINTs on three different parents — easy to verify
    // path-correctness because each tx is keyed by its own txid.
    let (p1, _) = fake_parent_outpoint(40);
    let (p2, _) = fake_parent_outpoint(41);
    let (p3, _) = fake_parent_outpoint(42);
    let tx1 = build_protostone_tx(p1, 0, 0x1, PROTOSTONE_OP_MINT, 0xAAAA);
    let tx2 = build_protostone_tx(p2, 0, 0x2, PROTOSTONE_OP_MINT, 0xBBBB);
    let tx3 = build_protostone_tx(p3, 0, 0x3, PROTOSTONE_OP_MINT, 0xCCCC);
    let (t1, t2, t3) = (tx1.compute_txid(), tx2.compute_txid(), tx3.compute_txid());

    // Chain A: txs in order [tx1, tx2, tx3] at height 2.
    let tx1a = tx1.clone();
    let tx2a = tx2.clone();
    let tx3a = tx3.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx1a.clone(), tx2a.clone(), tx3a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    let order_a = get_txids_at_height(&storage, 2, 5)?.unwrap_or_default();
    // 4 txs at height 2: coinbase + tx1 + tx2 + tx3 → 4 * 32 = 128 bytes.
    assert_eq!(order_a.len(), 4 * 32);
    let tx1_pos_a = &order_a[1 * 32..2 * 32];
    let tx2_pos_a = &order_a[2 * 32..3 * 32];
    let tx3_pos_a = &order_a[3 * 32..4 * 32];
    assert_eq!(tx1_pos_a, t1.as_byte_array());
    assert_eq!(tx2_pos_a, t2.as_byte_array());
    assert_eq!(tx3_pos_a, t3.as_byte_array());

    // Chain B: [tx3, tx1, tx2] at height 3 (different height + different order).
    let tx1b = tx1.clone();
    let tx2b = tx2.clone();
    let tx3b = tx3.clone();
    let chain_b = build_chain_with_txs(8, 6, move |h| {
        if h == 3 {
            vec![tx3b.clone(), tx1b.clone(), tx2b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // After reorg: at the new tip, all three outpoints exist (replay order
    // doesn't matter for independent MINTs).
    assert_eq!(
        get_outpoint(&storage, t1, 1, 6)?.as_deref(),
        Some(0xAAAAu64.to_le_bytes().as_slice())
    );
    assert_eq!(
        get_outpoint(&storage, t2, 1, 6)?.as_deref(),
        Some(0xBBBBu64.to_le_bytes().as_slice())
    );
    assert_eq!(
        get_outpoint(&storage, t3, 1, 6)?.as_deref(),
        Some(0xCCCCu64.to_le_bytes().as_slice())
    );

    // Per-height tx order at height 2 must be GONE (no protostone txs at
    // height 2 in chain B). Per-height tx order at height 3 must reflect
    // the new ordering [tx3, tx1, tx2].
    let order_b_h2 = get_txids_at_height(&storage, 2, 6)?.unwrap_or_default();
    // Height 2 in chain B has only the coinbase (1 tx -> 32 bytes).
    assert_eq!(
        order_b_h2.len(),
        32,
        "height 2 in chain B has only the coinbase"
    );

    let order_b_h3 = get_txids_at_height(&storage, 3, 6)?.unwrap_or_default();
    assert_eq!(order_b_h3.len(), 4 * 32);
    assert_eq!(
        &order_b_h3[32..64],
        t3.as_byte_array(),
        "first non-coinbase = tx3"
    );
    assert_eq!(&order_b_h3[64..96], t1.as_byte_array(), "second = tx1");
    assert_eq!(&order_b_h3[96..128], t2.as_byte_array(), "third = tx2");

    info!("✅ Scenario 4 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 5 — Deep reorg with mixed protostone activity.
// 10 chain-A blocks each carry a protostone; chain B forks at height 1
// and carries entirely different activity. After reorg, every chain-A
// tx must be GONE; every chain-B tx must be PRESENT.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_5_deep_reorg_mixed_activity() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 5: 10-block deep reorg, mixed protostone activity in both chains");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    // Generate 10 distinct parent outpoints for chain A and another 11 for B.
    let mut a_txs: Vec<Transaction> = Vec::new();
    for i in 0..10u64 {
        let (p, _) = fake_parent_outpoint(100 + i);
        a_txs.push(build_protostone_tx(
            p,
            0,
            i as u32,
            PROTOSTONE_OP_MINT,
            0xA000 + i,
        ));
    }
    let mut b_txs: Vec<Transaction> = Vec::new();
    for i in 0..11u64 {
        let (p, _) = fake_parent_outpoint(200 + i);
        b_txs.push(build_protostone_tx(
            p,
            0,
            i as u32,
            PROTOSTONE_OP_MINT,
            0xB000 + i,
        ));
    }

    let a_txs_clone = a_txs.clone();
    let chain_a = build_chain_with_txs(1, 10, move |h| vec![a_txs_clone[(h - 1) as usize].clone()]);
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 11).await?;

    // Sanity: every chain-A tx is present at the chain-A tip.
    for (i, tx) in a_txs.iter().enumerate() {
        let txid = tx.compute_txid();
        let val = get_outpoint(&storage, txid, 1, 10)?;
        assert_eq!(
            val.as_deref(),
            Some((0xA000u64 + i as u64).to_le_bytes().as_slice()),
            "chain A tx {} must be present pre-reorg",
            i
        );
    }

    let b_txs_clone = b_txs.clone();
    let chain_b =
        build_chain_with_txs(99, 11, move |h| vec![b_txs_clone[(h - 1) as usize].clone()]);
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 12).await?;

    // Post-reorg: every chain-A tx is GONE, every chain-B tx is PRESENT.
    for (i, tx) in a_txs.iter().enumerate() {
        let txid = tx.compute_txid();
        let val = get_outpoint(&storage, txid, 1, 11)?;
        assert!(
            val.is_none() || val.as_deref() == Some(&[][..]),
            "chain A tx {} must be GONE post-reorg; got: {:?}",
            i,
            val
        );
    }
    for (i, tx) in b_txs.iter().enumerate() {
        let txid = tx.compute_txid();
        let val = get_outpoint(&storage, txid, 1, 11)?;
        assert_eq!(
            val.as_deref(),
            Some((0xB000u64 + i as u64).to_le_bytes().as_slice()),
            "chain B tx {} must be present post-reorg",
            i
        );
    }

    info!("✅ Scenario 5 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 6 — Reorg of the parent of a CPFP child.
// Parent (MINT) + Child (TRANSFER spending parent's vout 1): both reorg
// out. State must reflect both being gone.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_6_cpfp_parent_reorg_invalidates_child() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 6: CPFP parent + child both reorg out");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    // Parent: MINT (creates dust at vout 1 of itself)
    let (parent_outp, parent_vout) = fake_parent_outpoint(60);
    let parent_tx = build_protostone_tx(
        parent_outp,
        parent_vout,
        0x60,
        PROTOSTONE_OP_MINT,
        0xC1C2C3C4,
    );
    let parent_txid = parent_tx.compute_txid();

    // Child: TRANSFER, spending parent's vout 1.
    let child_tx = build_protostone_tx(parent_txid, 1, 0x61, PROTOSTONE_OP_TRANSFER, 0xC1C2C3C4);
    let child_txid = child_tx.compute_txid();

    let parent_for_a = parent_tx.clone();
    let child_for_a = child_tx.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![parent_for_a.clone(), child_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    // Pre-reorg: parent's vout 1 has been emptied by the TRANSFER, child's
    // vout 1 holds the alkane.
    assert_eq!(
        get_outpoint(&storage, parent_txid, 1, 5)?.as_deref(),
        Some(&[][..]),
        "parent vout 1 must be emptied by child TRANSFER"
    );
    assert_eq!(
        get_outpoint(&storage, child_txid, 1, 5)?.as_deref(),
        Some(0xC1C2C3C4u64.to_le_bytes().as_slice()),
        "child vout 1 must hold the transferred alkane"
    );

    // Chain B forks at height 1, never includes parent or child.
    let chain_b = build_chain_with_txs(7, 6, move |_| vec![]);
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // Post-reorg: both parent and child outpoints must be zeroed.
    let parent_after = get_outpoint(&storage, parent_txid, 1, 6)?;
    let child_after = get_outpoint(&storage, child_txid, 1, 6)?;
    assert!(
        parent_after.is_none() || parent_after.as_deref() == Some(&[][..]),
        "parent vout 1 must be ZERO after parent reorgs out; got: {:?}",
        parent_after
    );
    assert!(
        child_after.is_none() || child_after.as_deref() == Some(&[][..]),
        "child vout 1 must be ZERO after parent reorgs out; got: {:?}",
        child_after
    );
    let parent_trace = get_trace(&storage, parent_txid, 6)?;
    let child_trace = get_trace(&storage, child_txid, 6)?;
    assert!(
        parent_trace.is_none() || parent_trace.as_deref() == Some(&[][..]),
        "parent trace must be ZERO after reorg; got: {:?}",
        parent_trace
    );
    assert!(
        child_trace.is_none() || child_trace.as_deref() == Some(&[][..]),
        "child trace must be ZERO after reorg; got: {:?}",
        child_trace
    );

    info!("✅ Scenario 6 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 7 — Reorg + same-block tx ordering swap.
// Tx A then Tx B in block N1 -> reorg -> Tx B then Tx A in N2 (same
// height, same txids, different order). State must reflect N2 order.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_7_same_height_tx_order_swap() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 7: same height + same txids, but flipped order across reorg");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    // Two independent MINTs at the same height. The block hash differs
    // between chains only because the coinbase salt differs — the protostone
    // txs are byte-identical.
    let (p1, _) = fake_parent_outpoint(70);
    let (p2, _) = fake_parent_outpoint(71);
    let tx_a = build_protostone_tx(p1, 0, 0x70, PROTOSTONE_OP_MINT, 0x7777);
    let tx_b = build_protostone_tx(p2, 0, 0x71, PROTOSTONE_OP_MINT, 0x8888);
    let txid_a = tx_a.compute_txid();
    let txid_b = tx_b.compute_txid();

    // Chain A: tx_a then tx_b at height 2, 5 blocks total.
    let tx_a_for_a = tx_a.clone();
    let tx_b_for_a = tx_b.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx_a_for_a.clone(), tx_b_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    let order_a = get_txids_at_height(&storage, 2, 5)?.unwrap_or_default();
    assert_eq!(order_a.len(), 3 * 32, "coinbase + tx_a + tx_b at h=2");
    assert_eq!(&order_a[32..64], txid_a.as_byte_array());
    assert_eq!(&order_a[64..96], txid_b.as_byte_array());

    // Chain B: same txs at the same height, flipped order, salt differs so
    // the block hash differs.
    let tx_a_for_b = tx_a.clone();
    let tx_b_for_b = tx_b.clone();
    let chain_b = build_chain_with_txs(2, 6, move |h| {
        if h == 2 {
            vec![tx_b_for_b.clone(), tx_a_for_b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    let order_b = get_txids_at_height(&storage, 2, 6)?.unwrap_or_default();
    assert_eq!(order_b.len(), 3 * 32);
    assert_eq!(
        &order_b[32..64],
        txid_b.as_byte_array(),
        "post-reorg: first non-coinbase must be tx_b"
    );
    assert_eq!(
        &order_b[64..96],
        txid_a.as_byte_array(),
        "post-reorg: second non-coinbase must be tx_a"
    );

    // Both MINTs still produce dust state, regardless of order.
    assert_eq!(
        get_outpoint(&storage, txid_a, 1, 6)?.as_deref(),
        Some(0x7777u64.to_le_bytes().as_slice())
    );
    assert_eq!(
        get_outpoint(&storage, txid_b, 1, 6)?.as_deref(),
        Some(0x8888u64.to_le_bytes().as_slice())
    );

    info!("✅ Scenario 7 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 8 — Reorg straddling a cellpack-version boundary.
//
// The metashrew-minimal protostone VM has no fork-gated logic — it
// dispatches purely on the opcode byte, never on height. The real
// alkanes runtime has had height-gated changes (e.g. opcode reinterp.
// at specific consensus heights), but that logic lives in the alkanes
// crates, not in metashrew. Documenting this gap explicitly so future
// readers don't try to reproduce it here.
//
// To still exercise the spirit of the test, we model it as: a tx whose
// opcode is INVALID (test VM falls into the "unknown opcode" path that
// writes an empty trace) in chain A, but VALID (proper MINT) in chain
// B — i.e. the "interpretation flips between branches" failure mode.
// Implementing this needs two distinct OP_RETURN payloads, which means
// two distinct txids — so we relax the "same tx, different
// interpretation" framing slightly.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_8_cellpack_version_boundary_documented_gap() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 8: cellpack-version boundary (documented gap — no fork-gated logic in test VM)");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, _) = fake_parent_outpoint(80);
    // tx_old uses an UNKNOWN opcode (the test WASM writes an empty trace
    // for any opcode it doesn't recognise but does still touch /trace/).
    let tx_old = build_protostone_tx(parent, 0, 0x80, 0xEE, 0x9999); // 0xEE unknown
    let txid_old = tx_old.compute_txid();
    // tx_new uses the standard MINT opcode.
    let tx_new = build_protostone_tx(parent, 0, 0x81, PROTOSTONE_OP_MINT, 0x9999);
    let txid_new = tx_new.compute_txid();

    let tx_old_for_a = tx_old.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx_old_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    // tx_old's trace must be the empty-trace marker; its outpoint untouched.
    let trace_old_a = get_trace(&storage, txid_old, 5)?;
    assert!(
        trace_old_a.as_deref() == Some(&[][..]),
        "unknown-opcode trace must be empty in chain A; got: {:?}",
        trace_old_a
    );
    assert!(
        get_outpoint(&storage, txid_old, 1, 5)?.is_none(),
        "unknown-opcode tx never writes outpoint state"
    );

    let tx_new_for_b = tx_new.clone();
    let chain_b = build_chain_with_txs(8, 6, move |h| {
        if h == 2 {
            vec![tx_new_for_b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // tx_new must mint into vout 1; tx_old (now gone) must have no trace.
    assert_eq!(
        get_outpoint(&storage, txid_new, 1, 6)?.as_deref(),
        Some(0x9999u64.to_le_bytes().as_slice())
    );
    let trace_old_b = get_trace(&storage, txid_old, 6)?;
    assert!(
        trace_old_b.is_none() || trace_old_b.as_deref() == Some(&[][..]),
        "tx_old's trace must be GONE after reorg; got: {:?}",
        trace_old_b
    );

    info!("✅ Scenario 8 passed (documented-gap version — see test docstring)");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 9 — Re-execute idempotency.
// Tx X mines in N1, reorgs to N2 with the SAME effects: state at
// outpoints(X) is identical (no drift).
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_9_idempotent_reexecution() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 9: same tx re-executed at a new height -> identical state");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, _) = fake_parent_outpoint(90);
    let tx_x = build_protostone_tx(parent, 0, 0x90, PROTOSTONE_OP_MINT, 0xABCD1234);
    let txid_x = tx_x.compute_txid();

    let tx_x_for_a = tx_x.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 2 {
            vec![tx_x_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;
    let state_a_at_5 = get_outpoint(&storage, txid_x, 1, 5)?;
    let trace_a_at_5 = get_trace(&storage, txid_x, 5)?;

    // Chain B: same tx at a different height, with a salt change so the
    // block hashes differ and the reorg actually triggers.
    let tx_x_for_b = tx_x.clone();
    let chain_b = build_chain_with_txs(99, 6, move |h| {
        if h == 4 {
            vec![tx_x_for_b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    let state_b_at_6 = get_outpoint(&storage, txid_x, 1, 6)?;
    let trace_b_at_6 = get_trace(&storage, txid_x, 6)?;

    // The actual values must be identical — same MINT into the same vout.
    assert_eq!(
        state_a_at_5, state_b_at_6,
        "outpoint state must be identical after idempotent re-execution"
    );
    assert_eq!(
        trace_a_at_5, trace_b_at_6,
        "trace state must be identical after idempotent re-execution"
    );

    info!("✅ Scenario 9 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Scenario 10 — "Already processed" guard does NOT block re-indexing.
// Same byte-identical tx at the same height, with chain B coming in
// later (block hash differs only via coinbase salt). The WASM MUST
// re-execute (else the user's wrap-tx fingerprint reproduces).
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_scenario_10_already_processed_guard_does_not_block_reindex() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Scenario 10: tx replays after reorg — state must come from the NEW execution");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, _) = fake_parent_outpoint(10);
    let tx_x = build_protostone_tx(parent, 0, 0xA0, PROTOSTONE_OP_MINT, 0x1010101010101010);
    let txid_x = tx_x.compute_txid();

    // Chain A: tx_x mined at height 3 with salt=1.
    let tx_x_for_a = tx_x.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 3 {
            vec![tx_x_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;

    // Capture chain-A height-2 snapshot (the tx isn't there yet) and
    // height-3 snapshot (the tx IS there). After reorg those snapshots
    // must reflect chain B's truth, not A's.
    let pre_outp_h2 = get_outpoint(&storage, txid_x, 1, 2)?;
    let pre_outp_h3 = get_outpoint(&storage, txid_x, 1, 3)?;
    assert!(
        pre_outp_h2.is_none() || pre_outp_h2.as_deref() == Some(&[][..]),
        "chain A: at h=2 the tx hasn't mined yet, outpoint must be empty"
    );
    assert_eq!(
        pre_outp_h3.as_deref(),
        Some(0x1010101010101010u64.to_le_bytes().as_slice())
    );

    // Chain B: byte-identical tx at the same height 3, salt=42.
    // Same txid (the tx is byte-identical), DIFFERENT block hash (coinbase
    // differs). This is the case that triggers the original failure
    // fingerprint — if the indexer treats "this tx was already processed"
    // as a skip-flag, the new chain's state ends up empty.
    let tx_x_for_b = tx_x.clone();
    let chain_b = build_chain_with_txs(42, 6, move |h| {
        if h == 3 {
            vec![tx_x_for_b.clone()]
        } else {
            vec![]
        }
    });
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 7).await?;

    // Post-reorg at the new tip: outpoint state must STILL be present.
    // If a skip-flag prevented re-execution, this assertion fails (the
    // fix is exactly the rollback that clears the append-only chain).
    let post_outp = get_outpoint(&storage, txid_x, 1, 6)?;
    assert_eq!(
        post_outp.as_deref(),
        Some(0x1010101010101010u64.to_le_bytes().as_slice()),
        "chain B: tx must be re-executed (skip-flag must not block re-index); got: {:?}",
        post_outp
    );

    // Per-height view at h=2 must STILL be empty (the tx mines at h=3 in
    // both chains, so h=2's view should not be polluted by either run).
    let post_outp_h2 = get_outpoint(&storage, txid_x, 1, 2)?;
    assert!(
        post_outp_h2.is_none() || post_outp_h2.as_deref() == Some(&[][..]),
        "historical view at h=2 must be empty in both chains; got: {:?}",
        post_outp_h2
    );

    // Per-height view at h=3 must reflect chain B's execution (not chain A
    // ghost-mins). Both write the same value, so equality is the marker.
    let post_outp_h3 = get_outpoint(&storage, txid_x, 1, 3)?;
    assert_eq!(
        post_outp_h3.as_deref(),
        Some(0x1010101010101010u64.to_le_bytes().as_slice()),
        "historical view at h=3 must reflect chain B's MINT"
    );

    // Per-height tx order at h=3 in chain B must contain tx_x.
    let order_b_h3 = get_txids_at_height(&storage, 3, 6)?.unwrap_or_default();
    assert_eq!(
        order_b_h3.len(),
        2 * 32,
        "coinbase + tx_x at h=3 in chain B"
    );
    assert_eq!(&order_b_h3[32..64], txid_x.as_byte_array());

    info!("✅ Scenario 10 passed");
    Ok(())
}

// -----------------------------------------------------------------------
// Extra: explicit zero-after-rollback sanity check.
//
// In addition to the ten scenarios above, this test verifies the lowest-
// level guarantee — that an SMT key written at height H is invisible at
// any historical-view snapshot strictly below H after rollback. If the
// append-only chain isn't trimmed during rollback (the bug from
// REORG_ROLLBACK_FIX.md), historical reads at the rollback height start
// to drift.
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_append_only_chain_trimmed_after_rollback() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Append-only chain trim sanity check");

    let config = TestConfig::new();
    let storage = MemStoreAdapter::new();

    let (parent, _) = fake_parent_outpoint(200);
    let tx_x = build_protostone_tx(parent, 0, 0xA0, PROTOSTONE_OP_MINT, 0xABCD);
    let txid_x = tx_x.compute_txid();

    let tx_x_for_a = tx_x.clone();
    let chain_a = build_chain_with_txs(1, 5, move |h| {
        if h == 4 {
            vec![tx_x_for_a.clone()]
        } else {
            vec![]
        }
    });
    let node = MockNode::new(chain_a.clone());
    sync_chain(&config, storage.clone(), node.clone(), 6).await?;
    // At chain-A tip, outpoint is set.
    assert_eq!(
        get_outpoint(&storage, txid_x, 1, 5)?.as_deref(),
        Some(0xABCDu64.to_le_bytes().as_slice())
    );

    // Chain B reorgs out the tx and goes 7 blocks deep.
    let chain_b = build_chain_with_txs(31, 7, move |_h| vec![]);
    node.switch_chain(chain_b.clone()).await;
    sync_chain(&config, storage.clone(), node, 8).await?;

    // Now query historically at every height — outpoint must be empty at
    // every height, because the only entry that ever wrote it was the
    // chain-A MINT, which has been rolled back.
    for h in 0..=7 {
        let val = get_outpoint(&storage, txid_x, 1, h)?;
        assert!(
            val.is_none() || val.as_deref() == Some(&[][..]),
            "historical view at h={} must be empty after reorg; got: {:?}",
            h,
            val
        );
    }

    info!("✅ Append-only-trim test passed");
    Ok(())
}
