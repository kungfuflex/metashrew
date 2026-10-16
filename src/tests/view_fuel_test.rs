//! Regression test: views must instantiate on the fuel-metered view engine.
//!
//! wasmtime 49 charges fuel during instantiation (constant expressions and
//! the start call). The view path creates a fresh store per call; before
//! v9.1.0-rc.2 it instantiated with 0 fuel, which wasmtime 43 tolerated and
//! wasmtime 49 turns into "all fuel consumed by WebAssembly" for every view.

use crate::test_utils::TestConfig;
use anyhow::Result;
use memshrew_runtime::MemStoreAdapter;

#[tokio::test]
async fn view_instantiates_with_fuel_metering() -> Result<()> {
    let config = TestConfig::new();
    let engine = wasmtime::Engine::default(); // ignored, see create_runtime
    let runtime = config
        .create_runtime_from_adapter(MemStoreAdapter::new(), engine)
        .await?;

    // Plain view path.
    let out = runtime.view("blocktracker".to_string(), &vec![], 0).await;
    assert!(out.is_ok(), "view() failed: {:?}", out.err());

    // Memory-capped view path (what rockshrew-mono's JSON-RPC uses).
    let limits = metashrew_runtime::view_limits::ViewLimitsConfig::default().view_store_limits();
    let out = runtime
        .view_with_limits("blocktracker".to_string(), &vec![], 0, Some(limits))
        .await;
    assert!(out.is_ok(), "view_with_limits() failed: {:?}", out.err());
    Ok(())
}
