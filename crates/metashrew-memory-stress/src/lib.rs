//! Memory stress test WASM module
//!
//! This module demonstrates non-determinism by attempting to allocate large amounts
//! of memory. The allocation can succeed or fail based on HOST memory constraints,
//! not just WASM memory limits, causing different execution results in different
//! environments (container with limited memory vs host with abundant memory).

use metashrew_core::{flush, index_pointer::IndexPointer, input};
use metashrew_support::{compat::export_bytes, index_pointer::KeyValuePointer};
use std::io::Cursor;
use std::sync::Arc;

/// Memory allocation configuration from input
struct MemoryConfig {
    /// Block height
    height: u32,
    /// Number of MB to attempt to allocate (per allocation)
    allocation_size_mb: u32,
    /// Number of allocations to make
    num_allocations: u32,
}

impl MemoryConfig {
    fn from_input(input: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(input);
        let height = metashrew_support::utils::consume_sized_int::<u32>(&mut cursor).unwrap_or(0);
        let allocation_size_mb =
            metashrew_support::utils::consume_sized_int::<u32>(&mut cursor).unwrap_or(100);
        let num_allocations =
            metashrew_support::utils::consume_sized_int::<u32>(&mut cursor).unwrap_or(10);

        Self {
            height,
            allocation_size_mb,
            num_allocations,
        }
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub fn _start() {
    let input_data = input();

    // If input is empty, just do a simple index (for normal block processing)
    if input_data.len() < 8 {
        simple_index(input_data);
        return;
    }

    // Parse configuration
    let config = MemoryConfig::from_input(input_data);

    // Store initial marker
    let mut marker = IndexPointer::from_keyword("/memory-stress-test");
    marker.set(Arc::new(b"started".to_vec()));

    // Attempt memory allocations
    let result = attempt_memory_stress(&config);

    // Store result
    let mut result_pointer =
        IndexPointer::from_keyword(&format!("/memory-stress-result/{}", config.height));

    match result {
        Ok(total_allocated) => {
            let success_msg = format!("SUCCESS:{}MB", total_allocated);
            result_pointer.set(Arc::new(success_msg.into_bytes()));

            // Store success marker
            let mut success = IndexPointer::from_keyword("/memory-stress-success");
            success.set(Arc::new(config.height.to_le_bytes().to_vec()));
        }
        Err(err_msg) => {
            let error_msg = format!("ERROR:{}", err_msg);
            result_pointer.set(Arc::new(error_msg.into_bytes()));

            // Store failure marker
            let mut failure = IndexPointer::from_keyword("/memory-stress-failure");
            failure.set(Arc::new(config.height.to_le_bytes().to_vec()));
        }
    }

    flush();
}

/// Simple indexing for normal block data
fn simple_index(block_data: Vec<u8>) {
    if block_data.len() < 4 {
        return;
    }

    let mut cursor = Cursor::new(block_data);
    let height = metashrew_support::utils::consume_sized_int::<u32>(&mut cursor).unwrap_or(0);
    let remaining = metashrew_support::utils::consume_to_end(&mut cursor).unwrap_or_default();

    let mut block_pointer = IndexPointer::from_keyword(&format!("/blocks/{}", height));
    block_pointer.set(Arc::new(remaining));
    flush();
}

/// Attempt to allocate and store large amounts of data
///
/// This function will succeed or fail based on HOST memory availability,
/// demonstrating non-determinism across different environments.
fn attempt_memory_stress(config: &MemoryConfig) -> Result<u32, String> {
    let mut total_allocated_mb = 0;
    let one_mb = 1024 * 1024;

    for i in 0..config.num_allocations {
        // Try to allocate memory
        let allocation_size = (config.allocation_size_mb as usize) * one_mb;

        // Attempt to create a large vector
        let data = match try_allocate(allocation_size) {
            Ok(d) => d,
            Err(e) => {
                return Err(format!(
                    "Failed at allocation {} of {}: {}",
                    i + 1,
                    config.num_allocations,
                    e
                ));
            }
        };

        // Store it in IndexPointer (this triggers host memory allocation via SMT)
        let key = format!("/memory-stress-data/{}/{}", config.height, i);
        let mut pointer = IndexPointer::from_keyword(&key);

        // This is the critical operation - storing large data in IndexPointer
        // can fail if the HOST is out of memory, even if WASM has space
        pointer.set(Arc::new(data));

        total_allocated_mb += config.allocation_size_mb;

        // Check if we're approaching WASM memory limit (4GB for wasm32)
        if total_allocated_mb > 3500 {
            return Err(format!(
                "Approaching WASM memory limit at {}MB",
                total_allocated_mb
            ));
        }
    }

    Ok(total_allocated_mb)
}

/// Try to allocate a vector of given size
///
/// This can fail for several reasons:
/// 1. WASM memory limit (4GB for wasm32)
/// 2. HOST memory limit (container or system)
/// 3. Wasmtime memory allocation failure
fn try_allocate(size: usize) -> Result<Vec<u8>, String> {
    // Try to create vector
    let mut data = Vec::new();

    // Reserve capacity - this is where allocation can fail
    if let Err(_) = data.try_reserve(size) {
        return Err(format!("Failed to reserve {}MB", size / (1024 * 1024)));
    }

    // Fill with data (using a pattern to avoid optimization)
    data.resize(size, 0x42);

    // Write pattern to ensure memory is actually allocated
    for i in (0..size).step_by(4096) {
        data[i] = (i & 0xFF) as u8;
    }

    Ok(data)
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn get_stress_result() -> i32 {
    let mut height_bytes = Cursor::new(input());
    let height = metashrew_support::utils::consume_sized_int::<u32>(&mut height_bytes).unwrap_or(0);

    let key = format!("/memory-stress-result/{}", height);
    let pointer = IndexPointer::from_keyword(&key);
    let result = pointer.get();

    export_bytes(result.as_ref().clone())
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn check_success() -> i32 {
    let pointer = IndexPointer::from_keyword("/memory-stress-success");
    let data = pointer.get();

    if data.is_empty() {
        0
    } else {
        1
    }
}

#[cfg(target_arch = "wasm32")]
#[no_mangle]
pub extern "C" fn check_failure() -> i32 {
    let pointer = IndexPointer::from_keyword("/memory-stress-failure");
    let data = pointer.get();

    if data.is_empty() {
        0
    } else {
        1
    }
}
