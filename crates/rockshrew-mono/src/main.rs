//! # Rockshrew-Mono: Combined Bitcoin Indexer and View Layer
//!
//! ## ARCHITECTURE OVERVIEW
//!
//! This is a monolithic Bitcoin indexer that combines both indexing and view layer functionality
//! into a single binary. It uses an **append-only database architecture** for reliable Bitcoin
//! blockchain indexing with full historical state access.
//!
//! ## APPEND-ONLY DATABASE DESIGN
//!
//! **IMPORTANT**: This system NO LONGER uses BST (Binary Search Tree) indexing. All BST code
//! has been removed as it was a flawed design. We now use a pure append-only approach:
//!
//! ### Key-Value Structure (v10 binary format):
//! - `"key/length"`: u32 LE — total number of updates for a key since indexing began
//! - `"key/0"`, `"key/1"`, `"key/2"`, etc.: Individual update entries
//! - Values stored as: `[u32 LE height | raw_value_bytes]` (v10; replaces the
//!   v9 `"{height}:{hex_value}"` UTF-8 string format — saves 50% on disk and
//!   removes per-read string parsing)
//!
//! ### Benefits:
//! - **Reorg Safety**: No data loss during blockchain reorganizations
//! - **Historical Access**: Binary search through updates for any block height
//! - **Consistency**: Deterministic state at any point in blockchain history
//!
//! ## CRATE HIERARCHY & CODE ORGANIZATION
//!
//! Code should be factored to the lowest common denominator in this hierarchy:
//!
//! ```
//! rockshrew-mono
//! ├── rockshrew-sync    (sync framework, adapters)
//! ├── rockshrew-runtime (RocksDB integration)
//! ├── metashrew-runtime (core WASM runtime, append-only SMT)
//! └── metashrew-core    (WASM bindings, fundamental types)
//! ```
//!
//! **Rule**: Always implement behavior in the lowest possible crate to maximize reusability.
//! Most core logic should live in `metashrew-runtime` and `metashrew-core`.

use anyhow::Result;
use clap::Parser;

/// Route every allocation in this process through jemalloc.
///
/// This must live in the BINARY crate — a `#[global_allocator]` declared in a
/// library has no effect on dependents, so putting it in `lib.rs` would be a
/// no-op for the shipped `rockshrew-mono` executable.
///
/// The dependency is built with `unprefixed_malloc_on_supported_platforms`, so
/// jemalloc also supplies the plain `malloc`/`free` symbols that the statically
/// linked RocksDB C++ core and libstdc++'s `operator new` resolve against. The
/// attribute below only covers Rust-side allocation; the unprefixed symbols are
/// what capture RocksDB.
///
/// Verify on the built binary with:
///   nm -D target/release/rockshrew-mono | grep -w ' T malloc'
///   MALLOC_CONF=stats_print:true ./rockshrew-mono --help   # prints a jemalloc report
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Initialize logging/tracing based on build features and environment
fn init_tracing() {
    // Check if console debugging is requested via environment variable
    let _use_console = std::env::var("ROCKSHREW_CONSOLE").is_ok();
    
    #[cfg(feature = "console")]
    if use_console {
        console_subscriber::init();
        return;
    }
    
    // Check if JSON tracing is requested
    #[cfg(feature = "debug-tracing")]
    if std::env::var("ROCKSHREW_JSON_TRACING").is_ok() {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().json())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        return;
    }
    
    // Enhanced tracing for debugging
    if std::env::var("ROCKSHREW_DEBUG").is_ok() {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
        
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_line_number(true)
                    .with_file(true)
            )
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
        return;
    }
    
    // Default: use env_logger for backward compatibility
    env_logger::builder().format_timestamp_secs().init();
}

/// Bump `RLIMIT_NOFILE` to a value that comfortably exceeds
/// RocksDB's `set_max_open_files(50000)` ceiling. The OS-level
/// ulimit gates the RocksDB internal cap — if RLIMIT_NOFILE is
/// at the typical default of 1024, RocksDB's table_cache hits
/// the ulimit first and commit_atomic fails with
/// `IO error: Too many open files` (production wedge 2026-05-21:
/// mork1e's node, block 892936 retried 600+ attempts).
///
/// We try 1_048_576 (1 << 20) first, which matches the standard
/// systemd unit-default for daemons that need lots of FDs. If the
/// hard limit is lower (containers / rootless runs), we set to
/// the hard cap. Returns the resulting soft limit so it gets
/// surfaced in startup logs.
fn raise_nofile_limit() -> std::io::Result<(u64, u64)> {
    let (soft_pre, hard) = rlimit::Resource::NOFILE.get()?;
    let target = 1_048_576u64.min(hard);
    if soft_pre < target {
        rlimit::Resource::NOFILE.set(target, hard)?;
    }
    let (soft_post, _hard) = rlimit::Resource::NOFILE.get()?;
    Ok((soft_pre, soft_post))
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    match raise_nofile_limit() {
        Ok((pre, post)) if post > pre => {
            log::info!(
                "raised RLIMIT_NOFILE soft cap from {} to {} (matches RocksDB \
                 set_max_open_files(50000) headroom; prevents the 'Too many \
                 open files' wedge documented in PRODUCTION_BUGS_AUDIT.md)",
                pre, post
            );
        }
        Ok((pre, _)) => {
            log::info!("RLIMIT_NOFILE soft cap already at {} (no bump needed)", pre);
        }
        Err(e) => {
            log::warn!(
                "failed to raise RLIMIT_NOFILE: {e}; if the indexer wedges on \
                 'Too many open files' during commit_atomic, raise it manually \
                 via `ulimit -n` or the container runtime"
            );
        }
    }

    let args = rockshrew_mono::Args::parse();
    rockshrew_mono::run_prod(args).await
}
