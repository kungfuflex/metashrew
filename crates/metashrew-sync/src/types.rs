//! Common types for rockshrew-sync

use serde::{Deserialize, Serialize};

/// Block processing result for the pipeline
#[derive(Debug, Clone)]
pub enum BlockResult {
    /// Block was successfully processed
    Success(u32),
    /// Block processing failed
    Error(u32, String),
}

/// Configuration for the sync process
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Starting block height
    pub start_block: u32,
    /// Optional exit block height
    pub exit_at: Option<u32>,
    /// Pipeline size for parallel processing
    pub pipeline_size: Option<usize>,
    /// Maximum reorg depth to handle
    pub max_reorg_depth: u32,
    /// Reorg check threshold (blocks from tip)
    pub reorg_check_threshold: u32,
    /// v9.0.5-rc.6: at `init()` time, defensively heal divergent on-disk
    /// pointers from prior crash-looping runs before letting sync advance.
    /// Reads three independent height pointers (the sync-engine indexed
    /// height, the runtime-side tip-height key, and the highest stored
    /// block-hash record). When they disagree, picks the minimum
    /// (conservative — re-applies some blocks rather than skipping any),
    /// validates that height against bitcoind, walks back further if the
    /// stored hash diverges from canonical, then issues a single atomic
    /// rollback that brings all three pointers into line.
    ///
    /// When `false`, init keeps the rc.5 behaviour: trusts whatever
    /// `__INTERNAL/height` says and proceeds. Useful for operators who
    /// want to inspect divergent state manually before letting the
    /// indexer advance.
    pub enable_startup_heal: bool,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            start_block: 0,
            exit_at: None,
            pipeline_size: None,
            max_reorg_depth: 100,
            reorg_check_threshold: 6,
            enable_startup_heal: true,
        }
    }
}

/// JSON-RPC request structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonRpcRequest {
    pub id: u32,
    pub jsonrpc: String,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

/// JSON-RPC response structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonRpcResponse {
    pub id: u32,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
    pub jsonrpc: String,
}

/// JSON-RPC error structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

/// View function call parameters
#[derive(Debug, Clone)]
pub struct ViewCall {
    pub function_name: String,
    pub input_data: Vec<u8>,
    pub height: u32,
}

/// View function result
#[derive(Debug, Clone)]
pub struct ViewResult {
    pub data: Vec<u8>,
}

/// Preview function call parameters (includes block data)
#[derive(Debug, Clone)]
pub struct PreviewCall {
    pub block_data: Vec<u8>,
    pub function_name: String,
    pub input_data: Vec<u8>,
    pub height: u32,
}

/// Block information
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub height: u32,
    pub hash: Vec<u8>,
    pub data: Vec<u8>,
}

/// Chain tip information
#[derive(Debug, Clone)]
pub struct ChainTip {
    pub height: u32,
    pub hash: Vec<u8>,
}

/// State root information
#[derive(Debug, Clone)]
pub struct StateRoot {
    pub height: u32,
    pub root: Vec<u8>,
}
