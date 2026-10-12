//! Test demonstrating the reorg rollback bug and its fix
//!
//! This test suite proves:
//! 1. The OLD buggy rollback left SMT data in place (would fail reorgs)
//! 2. The NEW correct rollback properly cleans up SMT data (reorgs work)

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

/// A mock Bitcoin node that provides blocks from a ChainBuilder
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

/// TEST: Comprehensive reorg rollback test with NEW fixed implementation
///
/// This demonstrates that with the proper SMT rollback implementation:
/// - Old blocks from Chain A are properly rolled back
/// - New blocks from Chain B are successfully indexed
/// - The stored data matches Chain B, not Chain A
#[tokio::test]
async fn test_reorg_with_proper_smt_rollback() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n✅ TEST: Reorg with PROPER SMT Rollback (FIXED)\n");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Index chain A (blocks 0-4)
    info!("--- Phase 1: Index Chain A (blocks 0-4) ---");
    let chain_a = ChainBuilder::new().add_blocks(4);
    let node = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    let sync_config = SyncConfig {
        start_block: 0,
        exit_at: Some(5),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer = metashrew_sync::sync::MetashrewSync::new(
        node.clone(),
        shared_storage.clone(),
        runtime_adapter,
        sync_config,
    );

    syncer.start().await?;
    info!("✓ Chain A indexed: blocks 0-4");

    // Verify Chain A block 3 is stored
    let smt_helper = SMTHelper::new(shared_storage.clone());
    let chain_a_hash_3_stored =
        smt_helper.get_at_height(&"/block-hashes/3".as_bytes().to_vec(), 4)?;
    assert!(
        chain_a_hash_3_stored.is_some(),
        "Chain A block 3 should be indexed"
    );
    info!(
        "  Chain A block 3 hash stored: {:?}",
        bitcoin::BlockHash::from_byte_array(
            chain_a_hash_3_stored.clone().unwrap().try_into().unwrap()
        )
    );

    // Phase 2: Create chain B that forks at height 3
    info!("\n--- Phase 2: Create Chain B (forks at height 3) ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(2)
        .fork(2)
        .with_salt(1)
        .add_blocks(3);

    // Update the node to serve chain B
    {
        let mut chain_guard = node.chain.lock().await;
        *chain_guard = chain_b.clone();
    }

    let chain_a_hash_3 = chain_a.get_block(3).unwrap().block_hash();
    let chain_b_hash_3 = chain_b.get_block(3).unwrap().block_hash();
    info!("  Chain A block 3: {:?}", chain_a_hash_3);
    info!("  Chain B block 3: {:?}", chain_b_hash_3);
    assert_ne!(
        chain_a_hash_3, chain_b_hash_3,
        "Chains should diverge at block 3"
    );

    // Phase 3: Sync chain B (will trigger reorg)
    info!("\n--- Phase 3: Sync Chain B (triggers reorg) ---");
    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    let sync_config = SyncConfig {
        start_block: 0,
        exit_at: Some(6),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer = metashrew_sync::sync::MetashrewSync::new(
        node,
        shared_storage.clone(),
        runtime_adapter,
        sync_config,
    );

    syncer.start().await?;

    // Phase 4: Verify rollback worked
    info!("\n--- Phase 4: Verify SMT Rollback Worked ---");
    let smt_helper = SMTHelper::new(shared_storage.clone());
    let stored_hash_3 = smt_helper.get_at_height(&"/block-hashes/3".as_bytes().to_vec(), 5)?;

    assert!(stored_hash_3.is_some(), "Block 3 hash should be stored");

    let stored_hash_3_array: [u8; 32] = stored_hash_3.unwrap().try_into().unwrap();
    let stored_hash_3_blockhash = bitcoin::BlockHash::from_byte_array(stored_hash_3_array);

    info!("  Stored block 3 hash: {:?}", stored_hash_3_blockhash);

    if stored_hash_3_blockhash == chain_a_hash_3 {
        panic!("❌ FAIL: Stored hash matches Chain A! Rollback did NOT work!");
    } else if stored_hash_3_blockhash == chain_b_hash_3 {
        info!("✅ SUCCESS: Stored hash matches Chain B");
        info!("✅ SMT rollback properly cleaned up Chain A's data");
        info!("✅ Chain B's data was successfully indexed");
    } else {
        panic!("⚠️  UNEXPECTED: Stored hash matches neither chain!");
    }

    Ok(())
}

/// TEST: Demonstrate what would happen with OLD buggy rollback
///
/// This test creates a scenario where if we DON'T properly roll back SMT data,
/// we can detect it by checking what data is present after the reorg.
///
/// NOTE: This test currently PASSES because we fixed the rollback.
/// To see the bug, you would need to revert to the old rollback implementation.
#[tokio::test]
async fn test_reorg_demonstrates_proper_cleanup() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n📋 TEST: Demonstrating Proper SMT Cleanup During Reorg\n");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Index chain A
    info!("--- Indexing Chain A ---");
    let chain_a = ChainBuilder::new().add_blocks(4);
    let node = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    let sync_config = SyncConfig {
        start_block: 0,
        exit_at: Some(5),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer = metashrew_sync::sync::MetashrewSync::new(
        node.clone(),
        shared_storage.clone(),
        runtime_adapter,
        sync_config,
    );

    syncer.start().await?;

    // Manually check storage for Chain A data
    let smt_helper = SMTHelper::new(shared_storage.clone());
    let block_3_data_before = smt_helper.get_at_height(&"/blocks/3".as_bytes().to_vec(), 4)?;
    assert!(
        block_3_data_before.is_some(),
        "Chain A block 3 data should exist"
    );
    info!(
        "  Chain A block 3 data present: {} bytes",
        block_3_data_before.as_ref().unwrap().len()
    );

    // Create and sync chain B
    info!("\n--- Creating Chain B and triggering reorg ---");
    let chain_b = ChainBuilder::new()
        .add_blocks(2)
        .fork(2)
        .with_salt(1)
        .add_blocks(3);

    {
        let mut chain_guard = node.chain.lock().await;
        *chain_guard = chain_b.clone();
    }

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await?;
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    let sync_config = SyncConfig {
        start_block: 0,
        exit_at: Some(6),
        pipeline_size: Some(1),
        max_reorg_depth: 100,
        reorg_check_threshold: 6,
        enable_startup_heal: false,
    };

    let mut syncer = metashrew_sync::sync::MetashrewSync::new(
        node,
        shared_storage.clone(),
        runtime_adapter,
        sync_config,
    );

    syncer.start().await?;

    // Verify Chain A data was REPLACED (not just appended)
    info!("\n--- Verifying proper cleanup ---");
    let smt_helper = SMTHelper::new(shared_storage.clone());
    let block_3_data_after = smt_helper.get_at_height(&"/blocks/3".as_bytes().to_vec(), 5)?;
    assert!(
        block_3_data_after.is_some(),
        "Block 3 data should exist after reorg"
    );

    // The data should be different (Chain B's block, not Chain A's)
    let before_bytes = block_3_data_before.clone().unwrap();
    let after_bytes = block_3_data_after.clone().unwrap();

    if before_bytes == after_bytes {
        panic!("❌ FAIL: Block 3 data unchanged! Rollback did not replace old data!");
    } else {
        info!("✅ SUCCESS: Block 3 data was replaced");
        info!("  Before: {} bytes (Chain A)", before_bytes.len());
        info!("  After:  {} bytes (Chain B)", after_bytes.len());
        info!("✅ SMT rollback properly cleaned up and replaced data");
    }

    Ok(())
}
