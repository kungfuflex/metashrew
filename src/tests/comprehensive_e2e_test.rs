//! Test demonstrating reorg rollback failure
//!
//! This test proves that when a reorganization occurs, the metashrew indexer
//! does NOT properly roll back state from reorged blocks before indexing the
//! new blocks at those heights.
//!
//! Expected behavior:
//! 1. Index chain A blocks 0-4
//! 2. Reorg: chain B provides different blocks at heights 3-4
//! 3. Before indexing new block 3, old block 3's state should be rolled back
//! 4. Index new blocks 3-5 on chain B
//!
//! Actual behavior (BUG):
//! 1. Index chain A blocks 0-4 ✓
//! 2. Reorg detected ✓
//! 3. Try to index new block 3 from chain B
//! 4. PANIC: Block 3's state from chain A is still present!
//! 5. This proves rollback is not working
//!
//! The WASM indexer now panics if we try to set a block height that's already
//! been set, which exposes this bug.

use crate::block_builder::ChainBuilder;
use crate::TestConfig;
use anyhow::Result;
use async_trait::async_trait;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use log::info;
use memshrew_runtime::MemStoreAdapter;
use metashrew_sync::{
    adapters::MetashrewRuntimeAdapter, BitcoinNodeAdapter, BlockInfo, ChainTip, SyncConfig,
    SyncEngine, SyncResult,
};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A mock Bitcoin node that provides blocks from a ChainBuilder
/// Can switch between different chains to simulate reorgs
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

/// TEST: Verifies reorg rollback behavior
///
/// Chain A: 0 -> 1 -> 2 -> 3 -> 4
/// Chain B: 0 -> 1 -> 2 -> 3' -> 4' -> 5' (different blocks at 3-5)
///
/// Expected: When syncing chain B, old state from blocks 3-4 should be
///           rolled back BEFORE indexing new blocks 3'-5'
///
/// This test verifies that reorg rollback is working by checking
/// which block hashes are stored after syncing both chains.
///
/// Result: ✅ The reorg rollback IS working! Block 3 from Chain A is
/// properly rolled back and replaced with Block 3 from Chain B.
#[tokio::test]
async fn test_reorg_rollback_works() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n✅ TEST: Verifying Reorg Rollback Works\n");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Phase 1: Index chain A (blocks 0-4)
    info!("--- Phase 1: Index Chain A (blocks 0-4) ---");
    let chain_a = ChainBuilder::new().add_blocks(4);
    let node = MockNode::new(chain_a.clone());

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine).unwrap();
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await
        .unwrap();
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

    syncer.start().await.unwrap();
    info!("✓ Chain A indexed: blocks 0-4");
    info!(
        "  Block 3 hash: {:?}",
        chain_a.get_block(3).unwrap().block_hash()
    );
    info!(
        "  Block 4 hash: {:?}",
        chain_a.get_block(4).unwrap().block_hash()
    );

    // Phase 2: Reorg - Switch node to Chain B which forks at height 3
    info!("\n--- Phase 2: Reorg - Chain B (forks at height 3) ---");
    info!("Chain B will have different blocks at heights 3, 4, 5");
    let chain_b = ChainBuilder::new()
        .add_blocks(2) // 0, 1, 2 (same as chain A)
        .fork(2) // Fork at height 2, so next block will be 3
        .with_salt(1) // Different blocks
        .add_blocks(3); // 3', 4', 5' (new blocks)

    node.switch_chain(chain_b.clone()).await;
    info!(
        "  New block 3' hash: {:?}",
        chain_b.get_block(3).unwrap().block_hash()
    );
    info!(
        "  New block 4' hash: {:?}",
        chain_b.get_block(4).unwrap().block_hash()
    );
    info!(
        "  New block 5' hash: {:?}",
        chain_b.get_block(5).unwrap().block_hash()
    );

    // Create new runtime for the continued sync
    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);
    let engine = wasmtime::Engine::new(&config_engine).unwrap();
    let runtime = config
        .create_runtime_from_adapter(shared_storage.clone(), engine)
        .await
        .unwrap();
    let runtime_adapter = MetashrewRuntimeAdapter::new(runtime);

    // Update syncer with new runtime adapter
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

    info!("\n--- Phase 3: Attempting to continue sync with Chain B ---");
    info!("⚠️  Expected: Reorg should be detected at height 3");
    info!("⚠️  Expected: State from old block 3 should be rolled back");
    info!("⚠️  Expected: New block 3' should be indexed successfully\n");

    syncer.start().await?;

    info!("\n--- Phase 4: Verify which blocks are stored ---");

    // Check what block hashes are actually stored
    use metashrew_runtime::smt::SMTHelper;
    let smt_helper = SMTHelper::new(shared_storage.clone());

    // Get stored block hashes at height 3, 4, 5
    let stored_hash_3 = smt_helper.get_at_height(&"/block-hashes/3".as_bytes().to_vec(), 5)?;
    let stored_hash_4 = smt_helper.get_at_height(&"/block-hashes/4".as_bytes().to_vec(), 5)?;
    let stored_hash_5 = smt_helper.get_at_height(&"/block-hashes/5".as_bytes().to_vec(), 5)?;

    // Convert stored hashes to hex for comparison
    let chain_a_hash_3 = chain_a.get_block(3).unwrap().block_hash();
    let chain_a_hash_4 = chain_a.get_block(4).unwrap().block_hash();
    let chain_b_hash_3 = chain_b.get_block(3).unwrap().block_hash();
    let chain_b_hash_4 = chain_b.get_block(4).unwrap().block_hash();
    let chain_b_hash_5 = chain_b.get_block(5).unwrap().block_hash();

    info!("Chain A block 3 hash: {:?}", chain_a_hash_3);
    info!("Chain B block 3 hash: {:?}", chain_b_hash_3);

    if let Some(hash_3_bytes) = stored_hash_3 {
        let stored_hash_3_array: [u8; 32] = hash_3_bytes.try_into().unwrap();
        let stored_hash_3_blockhash = bitcoin::BlockHash::from_byte_array(stored_hash_3_array);
        info!("Stored block 3 hash: {:?}", stored_hash_3_blockhash);

        if stored_hash_3_blockhash == chain_a_hash_3 {
            info!("❌ FAIL: Stored hash matches Chain A (rollback did NOT work!)");
            panic!("REORG ROLLBACK FAILURE: Block 3 from Chain A was not rolled back!");
        } else if stored_hash_3_blockhash == chain_b_hash_3 {
            info!("✅ SUCCESS: Stored hash matches Chain B");
            info!("✅ Reorg rollback is working correctly!");
            info!("✅ Old blocks from Chain A were properly rolled back");
            info!("✅ New blocks from Chain B were successfully indexed\n");
        } else {
            info!("⚠️  UNEXPECTED: Stored hash matches neither chain!");
            panic!("Unexpected stored hash - matches neither chain A nor chain B");
        }
    } else {
        info!("⚠️  Block 3 hash not found in storage");
        panic!("Block 3 hash not found in storage after sync");
    }

    Ok(())
}

/// CONTROL TEST: Normal operation without reorg should work fine
///
/// This test verifies that our panic check doesn't break normal operation.
/// When we index blocks in sequence without any reorg, everything should work.
#[tokio::test]
async fn test_normal_indexing_without_reorg() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n✅ CONTROL TEST: Normal indexing (no reorg)\n");

    let config = TestConfig::new();
    let shared_storage = MemStoreAdapter::new();

    // Just index a simple chain
    let chain = ChainBuilder::new().add_blocks(5);
    let node = MockNode::new(chain.clone());

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
    info!("✓ Normal indexing works fine (blocks 0-5)");
    info!("✅ Control test passed!\n");

    Ok(())
}
