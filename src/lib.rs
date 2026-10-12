//! Metashrew Test Suite
//!
//! This crate provides comprehensive testing for the Metashrew indexer framework
//! using the memshrew in-memory adapter and metashrew-minimal WASM module.

pub mod block_builder;
pub mod in_memory_adapters;
pub mod test_utils;
pub use test_utils::*;

#[cfg(test)]
pub mod tests;
