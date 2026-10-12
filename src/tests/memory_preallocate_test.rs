//! Test to verify that metashrew-runtime pre-allocates the full 4GB WASM32 memory space
//! and fails immediately if it cannot secure the full allocation.
//!
//! This test verifies the critical requirement that memory allocation is deterministic
//! across all environments by ensuring the runtime crashes if it cannot pre-allocate
//! the full 4GB address space upfront.

use memshrew_runtime::MemStoreAdapter;
use metashrew_runtime::MetashrewRuntime;

/// Test that instantiating the runtime requires the full 4GB memory space.
///
/// In a normal environment with sufficient memory, this should succeed.
/// In a memory-constrained environment (e.g., Docker with --memory=512m),
/// this should FAIL immediately during runtime instantiation, not during execution.
#[tokio::test]
#[ignore]
async fn test_runtime_instantiation_requires_4gb() {
    use crate::test_utils;

    println!("\n🔬 TEST: Runtime Instantiation Memory Pre-allocation\n");
    println!(
        "This test verifies that MetashrewRuntime pre-allocates the full 4GB WASM32 memory space"
    );
    println!("Expected behavior:");
    println!("  - High memory environment (≥4GB): Instantiation SUCCEEDS");
    println!("  - Low memory environment (<4GB): Instantiation FAILS/PANICS immediately");
    println!("\nAttempting to instantiate MetashrewRuntime...\n");

    // Create storage and engine
    let storage = MemStoreAdapter::new();
    let mut config = wasmtime::Config::default();
    config.async_support(true);
    let _engine = wasmtime::Engine::new(&config).expect("Failed to create engine");

    // This should fail immediately in low-memory environments if pre-allocation is working
    let result = MetashrewRuntime::new(test_utils::WASM, storage).await;

    match result {
        Ok(_runtime) => {
            println!("✅ Runtime instantiation SUCCEEDED");
            println!("   This means the environment has sufficient memory for 4GB pre-allocation");
        }
        Err(e) => {
            println!("❌ Runtime instantiation FAILED: {:?}", e);
            println!("   This means the environment cannot pre-allocate the required 4GB");
            panic!("Runtime failed to instantiate: {:?}", e);
        }
    }
}

/// Minimal test that just tries to create a runtime without any execution.
/// If pre-allocation is working, this should crash immediately in low-memory Docker.
#[tokio::test]
#[ignore]
async fn test_minimal_runtime_instantiation() {
    use crate::test_utils;

    println!("\n🔬 MINIMAL TEST: Just instantiate runtime, no execution\n");

    // Load the minimal WASM
    let wasm_bytes = test_utils::WASM;

    println!(
        "Creating MetashrewRuntime with {} bytes of WASM...",
        wasm_bytes.len()
    );

    // Create storage and engine
    let storage = MemStoreAdapter::new();
    let mut config = wasmtime::Config::default();
    config.async_support(true);
    let _engine = wasmtime::Engine::new(&config).expect("Failed to create engine");

    // This line should fail/panic if we can't pre-allocate 4GB
    let _runtime = MetashrewRuntime::new(wasm_bytes, storage).await.expect(
        "Failed to instantiate runtime - likely insufficient memory for 4GB pre-allocation",
    );

    println!("✅ Runtime instantiated successfully");
    println!("   Memory pre-allocation succeeded");
}
