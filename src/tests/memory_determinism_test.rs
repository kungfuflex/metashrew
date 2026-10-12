//! Memory Determinism Test
//!
//! This test demonstrates that WASM execution can produce different STORED DATA
//! based on HOST memory availability, violating determinism requirements.
//!
//! THE BUG: Both environments complete _start() successfully, but store different results
//!
//! The test:
//! 1. Runs a WASM module that attempts large memory allocations
//! 2. In a high-memory environment: allocations succeed → stores "SUCCESS:1000MB"
//! 3. In a low-memory environment: allocations fail → stores "ERROR:failed at allocation N"
//! 4. BOTH executions complete successfully (no panic), but produce DIFFERENT stored state
//! 5. This proves non-deterministic behavior: same input → different storage data

use anyhow::Result;
use log::{error, info, warn};
use memshrew_runtime::MemStoreAdapter;
use metashrew_runtime::{KeyValueStoreLike, MetashrewRuntime};
use std::path::PathBuf;

/// Configuration for memory stress test
#[derive(Debug, Clone)]
pub struct MemoryStressConfig {
    /// Block height to use
    pub height: u32,
    /// Size of each allocation in MB
    pub allocation_size_mb: u32,
    /// Number of allocations to attempt
    pub num_allocations: u32,
}

impl MemoryStressConfig {
    fn to_input_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(&self.allocation_size_mb.to_le_bytes());
        bytes.extend_from_slice(&self.num_allocations.to_le_bytes());
        bytes
    }
}

/// Load the memory stress WASM module
fn load_memory_stress_wasm() -> Result<Vec<u8>> {
    let wasm_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/wasm32-unknown-unknown/release/metashrew_memory_stress.wasm");

    if !wasm_path.exists() {
        return Err(anyhow::anyhow!(
            "Memory stress WASM not found at {:?}. Run: cargo build --target wasm32-unknown-unknown --release -p metashrew-memory-stress",
            wasm_path
        ));
    }

    std::fs::read(&wasm_path).map_err(|e| anyhow::anyhow!("Failed to read WASM: {}", e))
}

/// Run memory stress test with given configuration
async fn run_memory_stress_test(config: MemoryStressConfig) -> Result<String> {
    info!("Running memory stress test with config: {:?}", config);

    let wasm_bytes = load_memory_stress_wasm()?;
    info!("Loaded WASM: {} bytes", wasm_bytes.len());
    let storage = MemStoreAdapter::new();

    let mut config_engine = wasmtime::Config::default();
    config_engine.async_support(true);

    // Set memory limits for WASM
    // Max 4GB (WASM32 limit), but host constraints may prevent reaching this
    config_engine.max_wasm_stack(1024 * 1024); // 1MB stack

    let engine = wasmtime::Engine::new(&config_engine)?;
    let runtime = MetashrewRuntime::new(&wasm_bytes, storage.clone(), engine).await?;

    // Prepare input
    let input_bytes = config.to_input_bytes();
    info!(
        "Input bytes length: {}, data: {:?}",
        input_bytes.len(),
        input_bytes
    );

    // Set input in context
    {
        let mut context = runtime.context.write().await;
        context.block = input_bytes;
        context.height = config.height;
        context.db.set_height(config.height);
    }

    // Run the stress test
    info!("Executing WASM memory stress test...");
    let start = std::time::Instant::now();

    let result = runtime.run().await;
    let duration = start.elapsed();

    info!("WASM execution completed in {:?}", duration);

    // CRITICAL: We expect the WASM execution to ALWAYS succeed at the runtime level
    // The non-determinism is in what data gets stored, not whether execution completes
    match result {
        Ok(_) => {
            info!("✓ WASM _start() completed successfully");

            // Debug: Check various keys that might have been stored
            let test_keys = vec![
                "/memory-stress-test",
                "/memory-stress-result/1",
                "/memory-stress-success",
                "/memory-stress-failure",
                "/memory-stress-data/1/0",
            ];

            for test_key in &test_keys {
                let data = storage.get_immutable(&test_key.as_bytes().to_vec())?;
                info!(
                    "Key '{}': {:?}",
                    test_key,
                    data.as_ref().map(|d| {
                        if d.len() < 100 {
                            String::from_utf8_lossy(d).to_string()
                        } else {
                            format!("{} bytes", d.len())
                        }
                    })
                );
            }

            // Check what result the WASM stored using SMT helper
            use metashrew_runtime::smt::SMTHelper;
            let smt_helper = SMTHelper::new(storage.clone());

            let result_key = format!("/memory-stress-result/{}", config.height).into_bytes();
            info!(
                "Looking for result at key: {:?}",
                String::from_utf8_lossy(&result_key)
            );

            // Use SMT helper to get data at this height
            let result_data = smt_helper.get_at_height(&result_key, config.height)?;
            info!(
                "Result data (SMT at height {}): {:?}",
                config.height,
                result_data.as_ref().map(|d| d.len())
            );

            if let Some(data) = result_data {
                let result_str = String::from_utf8_lossy(&data);
                info!("WASM stored result: {}", result_str);

                if result_str.starts_with("SUCCESS") {
                    Ok(format!(
                        "WASM completed successfully, stored: {}",
                        result_str
                    ))
                } else if result_str.starts_with("ERROR") {
                    Ok(format!(
                        "WASM completed successfully, stored: {}",
                        result_str
                    ))
                } else {
                    Ok(format!(
                        "WASM completed successfully, stored: {}",
                        result_str
                    ))
                }
            } else {
                Ok("WASM completed successfully, no result stored".to_string())
            }
        }
        Err(e) => {
            error!("❌ WASM execution PANICKED at HOST level: {}", e);
            error!("This is NOT the non-determinism bug we're looking for!");
            error!("We expect _start() to complete successfully but store different results");
            Err(anyhow::anyhow!("WASM panic: {}", e))
        }
    }
}

/// Test: Run memory stress in normal environment
///
/// This test attempts to allocate significant memory and should document
/// whether it succeeds or fails.
#[tokio::test]
#[ignore] // Run with: cargo test --lib memory_stress_normal -- --ignored --nocapture
async fn test_memory_stress_normal_environment() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n🔬 TEST: Memory Stress in NORMAL Environment\n");

    let config = MemoryStressConfig {
        height: 1,
        allocation_size_mb: 50, // 50MB per allocation
        num_allocations: 20,    // 20 allocations = 1GB total
    };

    info!("This test runs in a NORMAL (unconstrained) environment");
    info!("Expected: Allocations should SUCCEED (if host has enough memory)");

    let result = run_memory_stress_test(config).await?;
    info!("\n📊 RESULT: {}\n", result);

    if result.contains("stored: SUCCESS") {
        info!("✅ Memory allocations SUCCEEDED in normal environment");
        info!("✅ WASM _start() completed and stored SUCCESS result");
    } else if result.contains("stored: ERROR") {
        warn!("⚠️  Memory allocations FAILED in this environment");
        warn!("⚠️  WASM _start() completed but stored ERROR result");
        warn!("⚠️  This demonstrates non-determinism: same input, different stored data!");
    } else {
        warn!("⚠️  Unexpected result format");
    }

    Ok(())
}

/// Test: Instructions for running in Docker container
///
/// This test provides instructions for running the stress test in a
/// memory-constrained Docker container.
#[test]
fn test_memory_stress_docker_instructions() {
    println!("\n📦 DOCKER CONTAINER MEMORY STRESS TEST\n");
    println!("To reproduce the non-determinism bug, run this test in a memory-constrained Docker container:\n");

    println!("IMPORTANT: Both environments will complete _start() successfully!");
    println!("The bug is that they store DIFFERENT DATA, not that one panics.\n");

    println!("1. Build the Docker image:");
    println!("   cd /data/metashrew");
    println!("   docker build -f tests/memory/Dockerfile -t metashrew-memory-test .\n");

    println!("2. Run with LIMITED memory (512MB):");
    println!("   docker run --memory=512m --memory-swap=512m metashrew-memory-test\n");

    println!("3. Run with NORMAL memory (4GB):");
    println!("   docker run --memory=4g metashrew-memory-test\n");

    println!("4. Compare the results:");
    println!("   - In 512MB container: WASM completes, stores 'ERROR:failed at allocation N'");
    println!("   - In 4GB container: WASM completes, stores 'SUCCESS:1000MB'");
    println!("   - BOTH complete successfully, but store DIFFERENT DATA!");
    println!("   - This proves NON-DETERMINISM: same input, different stored state!\n");

    println!("📝 See tests/memory/README.md for full documentation\n");
}

/// Helper: Get current memory usage (Linux only)
#[cfg(target_os = "linux")]
fn get_memory_info() -> Result<(u64, u64)> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let mut rss_kb = 0;
    let mut vm_size_kb = 0;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            rss_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
        if line.starts_with("VmSize:") {
            vm_size_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }

    Ok((rss_kb * 1024, vm_size_kb * 1024))
}

/// Test: Gradual memory increase to find the breaking point
#[tokio::test]
#[ignore] // Run with: cargo test --lib memory_stress_gradual -- --ignored --nocapture
async fn test_memory_stress_gradual_increase() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    info!("\n📈 TEST: Gradual Memory Increase to Find Breaking Point\n");

    // Start with small allocations and increase
    let test_configs = vec![
        (10, 10),  // 100MB total
        (50, 10),  // 500MB total
        (100, 10), // 1GB total
        (100, 20), // 2GB total
        (100, 30), // 3GB total
    ];

    for (allocation_mb, num_allocations) in test_configs {
        let config = MemoryStressConfig {
            height: 1,
            allocation_size_mb: allocation_mb,
            num_allocations,
        };

        let total_mb = allocation_mb * num_allocations;
        info!(
            "\n--- Testing: {}MB x {} = {}MB total ---",
            allocation_mb, num_allocations, total_mb
        );

        #[cfg(target_os = "linux")]
        {
            if let Ok((rss, vm_size)) = get_memory_info() {
                info!(
                    "Before test - RSS: {}MB, VmSize: {}MB",
                    rss / 1024 / 1024,
                    vm_size / 1024 / 1024
                );
            }
        }

        match run_memory_stress_test(config).await {
            Ok(result) => {
                info!("Result: {}", result);

                if result.contains("stored: ERROR") {
                    warn!("❌ Allocations FAILED at {}MB total", total_mb);
                    warn!("WASM completed but stored ERROR (breaking point found)");
                    break;
                } else if result.contains("stored: SUCCESS") {
                    info!("✅ Allocations SUCCEEDED at {}MB total", total_mb);
                    info!("WASM completed and stored SUCCESS");
                } else {
                    warn!("⚠️  Unexpected result at {}MB total", total_mb);
                }
            }
            Err(e) => {
                error!("❌ WASM PANICKED at {}MB total: {}", total_mb, e);
                error!("This is NOT the expected behavior - WASM should complete successfully");
                break;
            }
        }

        // Small delay between tests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    Ok(())
}
