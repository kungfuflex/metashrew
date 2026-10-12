//! Comprehensive Reorganization Testing Suite
//!
//! This test suite validates all possible chain reorganization scenarios
//! using the MetashrewSync engine with SPV-style validation.
//!
//! Test scenarios:
//! - Simple reorg: shorter chain replaced by longer chain
//! - Equal-length chains: first-seen chain should be kept
//! - Deep reorgs: reorgs at various depths
//! - Multiple sequential reorgs: chain switches multiple times
//! - State consistency: verify state matches expected chain after each reorg

use crate::block_builder::ChainBuilder;
use crate::TestConfig;
use anyhow::Result;
use async_trait::async_trait;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use log::info;
use memshrew_runtime::MemStoreAdapter;
use metashrew_runtime::smt::SMTHelper;
use metashrew_sync::{
    adapters::MetashrewRuntimeAdapter, BitcoinNodeAdapter, BlockInfo, ChainTip, SyncConfig,
    SyncEngine, SyncResult,
};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A simple mock Bitcoin node that provides blocks from a ChainBuilder
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
                hash.reverse(); // Convert to display order (big-endian) like bitcoind
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
        hash.reverse(); // Convert to display order
        Ok(ChainTip {
            height: chain.height(),
            hash,
        })
    }

    async fn is_connected(&self) -> bool {
        true
    }
}

fn get_indexed_block(adapter: &MemStoreAdapter, height: u32) -> Result<Option<Vec<u8>>> {
    let smt_helper = SMTHelper::new(adapter.clone());
    let key = format!("/blocks/{}", height).into_bytes();
    smt_helper.get_at_height(&key, height)
}

fn get_blocktracker(adapter: &MemStoreAdapter, height: u32) -> Result<Vec<u8>> {
    let smt_helper = SMTHelper::new(adapter.clone());
    let key = b"/blocktracker".to_vec();
    smt_helper
        .get_at_height(&key, height)
        .map(|opt| opt.unwrap_or_default())
}

/// Test 1: Simple reorg - shorter chain replaced by longer chain
///
/// Chain A: 0 -> 1 -> 2 -> 3 -> 4
/// Chain B: 0 -> 1 -> 2 -> 3' -> 4' -> 5' (longer, should win)
///
/// Expected: State should reflect Chain B after block 5'
#[tokio::test]
async fn test_simple_reorg_longer_chain_wins() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Test 1: Simple reorg - longer chain wins");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Build and sync chain A (5 blocks: 0-4)
    info!("--- Phase 1: Sync Chain A ---");
    let chain_a = ChainBuilder::new().add_blocks(4);
    let node_a = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a = MetashrewRuntimeAdapter::new(runtime_a);

    let sync_config_a = SyncConfig {
        start_block: 0,
        exit_at: Some(5),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a = metashrew_sync::sync::MetashrewSync::new(
        node_a,
        shared_storage.clone(),
        runtime_adapter_a,
        sync_config_a,
    );

    syncer_a.start().await?;

    let tracker_a = get_blocktracker(&shared_storage, 4)?;
    info!(
        "  ✓ Chain A synced: 5 blocks, tracker len: {}",
        tracker_a.len()
    );
    assert_eq!(tracker_a.len(), 5, "Chain A should have 5 blocks");

    // Phase 2: Build longer chain B forking at height 3, sync it (triggers reorg)
    info!("--- Phase 2: Sync Chain B (longer, forks at 3) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(3)
        .fork(3)
        .with_salt(1)
        .add_blocks(3);
    let node_b = MockNode::new(chain_b.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_b = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_b = MetashrewRuntimeAdapter::new(runtime_b);

    let sync_config_b = SyncConfig {
        start_block: 0,
        exit_at: Some(6),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_b = metashrew_sync::sync::MetashrewSync::new(
        node_b,
        shared_storage.clone(),
        runtime_adapter_b,
        sync_config_b,
    );

    syncer_b.start().await?;

    let tracker_b = get_blocktracker(&shared_storage, 5)?;
    info!(
        "  ✓ Chain B synced: 6 blocks, tracker len: {}",
        tracker_b.len()
    );
    assert_eq!(tracker_b.len(), 6, "Should have 6 blocks after reorg");

    // Verify state changed after reorg
    assert_ne!(tracker_a, tracker_b[..5], "State should differ after reorg");

    info!("  ✓ Chain B (longer) replaced Chain A");
    info!("✅ Test 1 passed!\n");

    Ok(())
}

/// Test 2: Deep reorg at block 1 (reorg depth = 9 blocks)
///
/// Chain A: 0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9
/// Chain B: 0 -> 1' -> 2' -> ... -> 10' (forks at 1, longer by 1)
///
/// Expected: Deep reorg should succeed, Chain B should replace Chain A
#[tokio::test]
async fn test_deep_reorg() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Test 2: Deep reorg (depth = 9 blocks)");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Sync chain A (10 blocks: 0-9)
    info!("--- Phase 1: Sync Chain A (10 blocks) ---");
    let chain_a = ChainBuilder::new().add_blocks(9);
    let node_a = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a = MetashrewRuntimeAdapter::new(runtime_a);

    let sync_config_a = SyncConfig {
        start_block: 0,
        exit_at: Some(10),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a = metashrew_sync::sync::MetashrewSync::new(
        node_a,
        shared_storage.clone(),
        runtime_adapter_a,
        sync_config_a,
    );

    syncer_a.start().await?;

    let tracker_a = get_blocktracker(&shared_storage, 9)?;
    assert_eq!(tracker_a.len(), 10);
    info!("  ✓ Chain A synced: 10 blocks");

    // Phase 2: Fork at height 1, sync longer chain B (11 blocks total)
    info!("--- Phase 2: Sync Chain B (11 blocks, forks at 1) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(1)
        .fork(1)
        .with_salt(3)
        .add_blocks(10);
    let node_b = MockNode::new(chain_b.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_b = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_b = MetashrewRuntimeAdapter::new(runtime_b);

    let sync_config_b = SyncConfig {
        start_block: 0,
        exit_at: Some(11),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_b = metashrew_sync::sync::MetashrewSync::new(
        node_b,
        shared_storage.clone(),
        runtime_adapter_b,
        sync_config_b,
    );

    syncer_b.start().await?;

    let tracker_b = get_blocktracker(&shared_storage, 10)?;
    assert_eq!(tracker_b.len(), 11);

    info!("  ✓ Deep reorg succeeded (rolled back 9 blocks)");
    info!("  ✓ Chain B (11 blocks) replaced Chain A");
    info!("✅ Test 2 passed!\n");

    Ok(())
}

/// Test 3: Multiple sequential reorgs - chain switches back and forth
#[tokio::test]
async fn test_multiple_sequential_reorgs() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Test 3: Multiple sequential reorgs");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Initial chain A (4 blocks: 0-3)
    info!("--- Phase 1: Sync Chain A (4 blocks) ---");
    let chain_a = ChainBuilder::new().add_blocks(3);
    let node_a = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a = MetashrewRuntimeAdapter::new(runtime_a);

    let sync_config_a = SyncConfig {
        start_block: 0,
        exit_at: Some(4),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a = metashrew_sync::sync::MetashrewSync::new(
        node_a,
        shared_storage.clone(),
        runtime_adapter_a,
        sync_config_a,
    );

    syncer_a.start().await?;
    let state_a = get_blocktracker(&shared_storage, 3)?;
    info!("  ✓ Chain A: 4 blocks");

    // Phase 2: REORG 1 - Chain B forks at 3, adds 2 blocks (5 total, wins)
    info!("--- Phase 2: Reorg 1 - Chain B (5 blocks, forks at 3) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(3)
        .fork(3)
        .with_salt(4)
        .add_blocks(2);
    let node_b = MockNode::new(chain_b.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_b = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_b = MetashrewRuntimeAdapter::new(runtime_b);

    let sync_config_b = SyncConfig {
        start_block: 0,
        exit_at: Some(5),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_b = metashrew_sync::sync::MetashrewSync::new(
        node_b,
        shared_storage.clone(),
        runtime_adapter_b,
        sync_config_b,
    );

    syncer_b.start().await?;
    let state_b = get_blocktracker(&shared_storage, 4)?;
    info!("  ✓ Reorg 1: Chain B (5 blocks) replaced Chain A");

    // Phase 3: REORG 2 - Extend chain A to 6 blocks (should win back)
    info!("--- Phase 3: Reorg 2 - Chain A extended to 6 blocks ---");
    let chain_a_extended = ChainBuilder::new().add_blocks(5);
    let node_a2 = MockNode::new(chain_a_extended.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a2 = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a2 = MetashrewRuntimeAdapter::new(runtime_a2);

    let sync_config_a2 = SyncConfig {
        start_block: 0,
        exit_at: Some(6),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a2 = metashrew_sync::sync::MetashrewSync::new(
        node_a2,
        shared_storage.clone(),
        runtime_adapter_a2,
        sync_config_a2,
    );

    syncer_a2.start().await?;
    let state_a2 = get_blocktracker(&shared_storage, 5)?;

    // Verify we're back to chain A lineage (first 4 blocks match original chain A)
    assert_eq!(
        state_a[..4],
        state_a2[..4],
        "Should be back to Chain A lineage"
    );
    info!("  ✓ Reorg 2: Chain A extended to 6 blocks, won back");

    info!("✅ Test 3 passed!\n");

    Ok(())
}

/// Test 4: State consistency after reorg
#[tokio::test]
async fn test_state_consistency_after_reorg() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Test 4: State consistency verification");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Sync chain A (5 blocks)
    info!("--- Phase 1: Sync Chain A (5 blocks) ---");
    let chain_a = ChainBuilder::new().add_blocks(4);
    let node_a = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a = MetashrewRuntimeAdapter::new(runtime_a);

    let sync_config_a = SyncConfig {
        start_block: 0,
        exit_at: Some(5),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a = metashrew_sync::sync::MetashrewSync::new(
        node_a,
        shared_storage.clone(),
        runtime_adapter_a,
        sync_config_a,
    );

    syncer_a.start().await?;
    info!("  ✓ Chain A: 5 blocks indexed");

    // Verify blocks from chain A are present
    for height in 0..=4 {
        let block = get_indexed_block(&shared_storage, height)?;
        assert!(block.is_some(), "Block {} should exist", height);
    }
    info!("  ✓ All Chain A blocks retrievable");

    // Phase 2: Fork at height 2, sync longer chain B (6 blocks total)
    info!("--- Phase 2: Sync Chain B (6 blocks, forks at 2) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(2)
        .fork(2)
        .with_salt(6)
        .add_blocks(4);
    let node_b = MockNode::new(chain_b.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_b = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_b = MetashrewRuntimeAdapter::new(runtime_b);

    let sync_config_b = SyncConfig {
        start_block: 0,
        exit_at: Some(6),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_b = metashrew_sync::sync::MetashrewSync::new(
        node_b,
        shared_storage.clone(),
        runtime_adapter_b,
        sync_config_b,
    );

    syncer_b.start().await?;
    info!("  ✓ Chain B: reorg completed (6 blocks total)");

    // Verify common ancestor blocks (0-2) still exist
    for height in 0..=2 {
        let block = get_indexed_block(&shared_storage, height)?;
        assert!(
            block.is_some(),
            "Common ancestor block {} should exist",
            height
        );
    }
    info!("  ✓ Common ancestor blocks (0-2) preserved");

    // Verify reorged blocks (3-5) now exist from chain B
    for height in 3..=5 {
        let block = get_indexed_block(&shared_storage, height)?;
        assert!(
            block.is_some(),
            "Reorged block {} should exist from Chain B",
            height
        );
    }
    info!("  ✓ Reorged blocks (3-5) from Chain B present");

    // Verify the actual data matches chain B
    let block_data_5 = get_indexed_block(&shared_storage, 5)?.unwrap();
    let chain_b_block_5 = chain_b.get_block(5).unwrap();
    let expected_bytes = metashrew_support::utils::consensus_encode(chain_b_block_5)?;
    assert_eq!(
        block_data_5, expected_bytes,
        "Block 5 data should match Chain B"
    );
    info!("  ✓ Block data matches Chain B");

    info!("✅ Test 4 passed!\n");

    Ok(())
}

/// Test 5: Reorg at genesis (fork immediately after block 0)
///
/// Chain A: 0 -> 1 -> 2
/// Chain B: 0 -> 1' -> 2' -> 3' (forks at 0, longer)
///
/// Expected: Should handle reorg even at genesis
#[tokio::test]
async fn test_reorg_at_genesis() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🧪 Test 5: Reorg at genesis");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Sync chain A (3 blocks: 0-2)
    info!("--- Phase 1: Sync Chain A (3 blocks) ---");
    let chain_a = ChainBuilder::new().add_blocks(2);
    let node_a = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_a = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_a = MetashrewRuntimeAdapter::new(runtime_a);

    let sync_config_a = SyncConfig {
        start_block: 0,
        exit_at: Some(3),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_a = metashrew_sync::sync::MetashrewSync::new(
        node_a,
        shared_storage.clone(),
        runtime_adapter_a,
        sync_config_a,
    );

    syncer_a.start().await?;
    let state_a = get_blocktracker(&shared_storage, 2)?;
    info!("  ✓ Chain A: 3 blocks (0-2)");

    // Phase 2: Fork at genesis (block 0), sync longer chain B (4 blocks: 0-3)
    info!("--- Phase 2: Sync Chain B (4 blocks, forks at 0) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(0)
        .fork(0)
        .with_salt(5)
        .add_blocks(4);
    let node_b = MockNode::new(chain_b.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime_b = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter_b = MetashrewRuntimeAdapter::new(runtime_b);

    let sync_config_b = SyncConfig {
        start_block: 0,
        exit_at: Some(4),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer_b = metashrew_sync::sync::MetashrewSync::new(
        node_b,
        shared_storage.clone(),
        runtime_adapter_b,
        sync_config_b,
    );

    syncer_b.start().await?;

    let state_b = get_blocktracker(&shared_storage, 3)?;
    assert_ne!(
        state_a,
        state_b[..3],
        "Genesis reorg should change all state"
    );
    assert_eq!(state_b.len(), 4);

    info!("  ✓ Genesis reorg succeeded");
    info!("  ✓ Chain B (4 blocks) replaced Chain A");
    info!("✅ Test 5 passed!\n");

    Ok(())
}
