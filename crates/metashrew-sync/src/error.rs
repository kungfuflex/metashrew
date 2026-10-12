//! Error types for rockshrew-sync

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Bitcoin node error: {0}")]
    BitcoinNode(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Chain reorganization error: {0}")]
    Reorg(String),

    #[error("Block processing error at height {height}: {message}")]
    BlockProcessing { height: u32, message: String },

    #[error("View function error: {0}")]
    ViewFunction(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    /// View runtime is temporarily refusing new work (v9.0.5-rc.2 view
    /// isolation). Returned when the view-call semaphore is saturated OR
    /// the host-memory floor is breached. The JSON-RPC handler maps this
    /// to a -32001 error code so callers can retry-with-backoff.
    #[error("View runtime unavailable: {0}")]
    Unavailable(String),

    /// View call exceeded its per-call resource budget (v9.0.5-rc.2 view
    /// isolation). Returned when the WASM linear memory grows past
    /// `--view-memory-mb`. The JSON-RPC handler maps this to a -32002
    /// error code; the call should NOT be retried unmodified.
    #[error("View runtime resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

pub type SyncResult<T> = Result<T, SyncError>;
