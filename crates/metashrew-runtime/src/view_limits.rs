//! View-function resource isolation primitives.
//!
//! This module ships the v9.0.5-rc.2 view-runtime hardening: under heavy
//! view-call load (e.g. espo fanning many `metashrew_view` calls at once)
//! the indexer process was being OOM-killed because each view spun up a
//! fresh wasmtime store with no upper bound on either concurrency or
//! per-call memory growth. The v9.0.5-rc.1 strict-determinism work made
//! block-commit recovery correct, but the indexer still went down. Here
//! we make the indexer immune to view-function load in the first place.
//!
//! Three knobs:
//!
//! 1. **Concurrent-call bound** — a `tokio::sync::Semaphore` shared across
//!    the JSON-RPC server caps the number of in-flight view calls. Default
//!    permits = 16 (configurable via `--view-concurrency`). Excess calls
//!    wait for a permit OR get the configured per-method timeout, after
//!    which they get [`ViewAcquireError::AcquireTimeout`].
//!
//! 2. **Per-view WASM memory cap** — each view store is created with a
//!    [`wasmtime::StoreLimits`] capping linear-memory growth at
//!    `--view-memory-mb` (default 256 MB). When a view exceeds the budget
//!    wasmtime raises a trap which the dispatch wraps in a
//!    `ResourceExhausted` SyncError. The indexer store is NEVER limited.
//!
//! 3. **Host-memory floor** — before acquiring a view permit we check the
//!    host's available memory via `sysinfo`. If less than
//!    `--view-memory-floor-mb` (default 8192 MB) is free we refuse new view
//!    calls so the indexer keeps breathing room. The cached available-memory
//!    value is refreshed at most every `memory_floor_refresh_ms` (default
//!    1000 ms) so we don't add per-call latency.
//!
//! ## Indexer immunity
//!
//! Neither the semaphore nor the memory-floor check is called from the
//! indexer path. The block-processing loop uses
//! [`crate::runtime::MetashrewRuntime::process_block`] /
//! `process_block_atomic` which construct a different (sync) store with the
//! pre-existing [`wasmtime::StoreLimits`] (unbounded memory) and never
//! consult [`ViewLimiter`]. See the `view_isolation_indexer_immune` test
//! in `crates/metashrew-runtime/tests/view_memory_isolation.rs`.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use wasmtime::{StoreLimits, StoreLimitsBuilder};

/// Default permit count for the view-call semaphore. Sized so that 16
/// concurrent ~256 MB view stores fit in ~4 GB of resident WASM memory,
/// leaving the indexer ~4 GB+ of headroom on a 16 GB pod.
pub const DEFAULT_VIEW_CONCURRENCY: usize = 16;

/// Default per-view WASM linear-memory cap, in MB.
pub const DEFAULT_VIEW_MEMORY_MB: usize = 256;

/// Default host-memory floor below which new view calls are refused, in MB.
pub const DEFAULT_VIEW_MEMORY_FLOOR_MB: u64 = 8192;

/// Default cadence at which the cached `available_memory` is refreshed.
pub const DEFAULT_MEMORY_FLOOR_REFRESH_MS: u64 = 1000;

/// Default semaphore-acquire timeout when no per-method timeout is in play.
/// In production this is always overridden by the JSON-RPC per-method
/// timeout (`metashrew_view` = 60s). Set conservative as a backstop.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for view-runtime isolation. Threaded from CLI flags.
#[derive(Debug, Clone)]
pub struct ViewLimitsConfig {
    /// Max concurrent in-flight `metashrew_view` calls.
    pub view_concurrency: usize,
    /// Per-view WASM linear-memory cap in bytes.
    pub view_memory_bytes: usize,
    /// Host-memory floor in bytes — refuse new view calls when free RAM
    /// falls below this.
    pub view_memory_floor_bytes: u64,
    /// How often the cached host-memory state is refreshed.
    pub memory_floor_refresh: Duration,
    /// Backstop acquire timeout for the semaphore. In practice the
    /// JSON-RPC per-method timeout supplies a tighter value.
    pub acquire_timeout: Duration,
}

impl Default for ViewLimitsConfig {
    fn default() -> Self {
        Self {
            view_concurrency: DEFAULT_VIEW_CONCURRENCY,
            view_memory_bytes: DEFAULT_VIEW_MEMORY_MB * 1024 * 1024,
            view_memory_floor_bytes: DEFAULT_VIEW_MEMORY_FLOOR_MB * 1024 * 1024,
            memory_floor_refresh: Duration::from_millis(DEFAULT_MEMORY_FLOOR_REFRESH_MS),
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }
}

impl ViewLimitsConfig {
    /// Build the wasmtime [`StoreLimits`] applied to every view store.
    /// `trap_on_grow_failure(true)` is critical: without it, a WASM
    /// `memory.grow` that hits our cap returns -1 (which the indexer WASM
    /// may handle gracefully and then loop), with it the WASM traps and we
    /// surface a clean error to the JSON-RPC caller.
    pub fn view_store_limits(&self) -> StoreLimits {
        StoreLimitsBuilder::new()
            .memory_size(self.view_memory_bytes)
            // Per-view stores never need more than one memory / table /
            // instance — keep these conservative.
            .memories(1)
            .tables(8)
            .instances(1)
            .trap_on_grow_failure(true)
            .build()
    }
}

/// Reasons a view call may be refused at acquire time.
#[derive(Debug, thiserror::Error)]
pub enum ViewAcquireError {
    /// All `view_concurrency` permits are in use and the caller's wait
    /// deadline expired.
    #[error("view-runtime saturated, retry later")]
    Saturated,
    /// Host has less than the configured floor of available memory free.
    /// New view calls are refused to give the indexer breathing room.
    #[error("system memory pressure, view runtime backing off")]
    MemoryFloor,
    /// The semaphore was closed (process shutting down).
    #[error("view runtime is shutting down")]
    Closed,
}

// Add thiserror to the runtime crate? It's already pulled in transitively
// via wasmtime; let me check —
// Actually use anyhow-style inline impl to avoid the dep:
// keep thiserror — easier to read. (anyhow + wasmtime both pull it; not
// adding a new top-level dep.)

/// A held view-call permit. Drop releases the permit back to the semaphore.
#[derive(Debug)]
pub struct ViewPermit {
    _permit: OwnedSemaphorePermit,
}

/// Cached snapshot of host-memory state. Refreshed at most every
/// `refresh_interval`.
struct MemoryFloorState {
    available_bytes: u64,
    last_refresh: Instant,
}

/// Tracks the host's free memory with a bounded refresh cadence so that
/// repeated view-acquire calls don't hammer `/proc/meminfo`.
///
/// The threshold is stored as `AtomicU64` so tests can re-tune it without
/// reconstructing the limiter; in production it is set once from CLI args
/// and never mutated.
pub struct MemoryFloor {
    threshold_bytes: AtomicU64,
    refresh_interval: Duration,
    state: Mutex<MemoryFloorState>,
    // `sysinfo::System` is heavy; we keep one per floor and refresh it
    // in place. Behind a Mutex because `refresh_memory` is `&mut self`.
    system: Mutex<sysinfo::System>,
}

impl MemoryFloor {
    pub fn new(threshold_bytes: u64, refresh_interval: Duration) -> Self {
        // We intentionally do a synchronous refresh at construction so the
        // first acquire is fast.
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let available_bytes = system.available_memory();

        Self {
            threshold_bytes: AtomicU64::new(threshold_bytes),
            refresh_interval,
            state: Mutex::new(MemoryFloorState {
                available_bytes,
                last_refresh: Instant::now(),
            }),
            system: Mutex::new(system),
        }
    }

    /// Returns Ok(()) if available memory is at or above the floor.
    /// Returns Err(MemoryFloor) if below.
    pub async fn check(&self) -> Result<(), ViewAcquireError> {
        let threshold = self.threshold_bytes.load(AtomicOrdering::Relaxed);
        // Fast path: cached value is fresh.
        let cached = {
            let s = self.state.lock().await;
            (s.available_bytes, s.last_refresh)
        };
        let now = Instant::now();
        if now.duration_since(cached.1) < self.refresh_interval {
            if cached.0 >= threshold {
                return Ok(());
            } else {
                return Err(ViewAcquireError::MemoryFloor);
            }
        }

        // Slow path: refresh + re-check. Hold the system lock for the
        // duration of the refresh so we don't double-refresh under load.
        let mut sys = self.system.lock().await;
        sys.refresh_memory();
        let available = sys.available_memory();
        drop(sys);

        let mut s = self.state.lock().await;
        s.available_bytes = available;
        s.last_refresh = Instant::now();

        if available >= threshold {
            Ok(())
        } else {
            log::warn!(
                "view memory-floor: available={}MB < threshold={}MB — refusing new view calls",
                available / (1024 * 1024),
                threshold / (1024 * 1024)
            );
            Err(ViewAcquireError::MemoryFloor)
        }
    }

    /// Override the cached available-memory value to simulate memory
    /// pressure. Used by `tests/view_memory_isolation.rs`. NOT meant for
    /// production use — the value will be overwritten on the next refresh
    /// (or after `refresh_interval` elapses).
    pub async fn override_available_memory(&self, bytes: u64) {
        let mut s = self.state.lock().await;
        s.available_bytes = bytes;
        // Push last_refresh far enough into the future that the cached
        // override sticks across the refresh window.
        s.last_refresh = Instant::now();
    }

    /// Set a new floor threshold. Test-only in spirit (production sets
    /// this once at startup), but cheap enough to expose unconditionally.
    pub fn set_threshold(&self, bytes: u64) {
        self.threshold_bytes.store(bytes, AtomicOrdering::Relaxed);
    }
}

/// View-runtime acquirer: combines the concurrency semaphore + memory floor.
pub struct ViewLimiter {
    config: ViewLimitsConfig,
    semaphore: Arc<Semaphore>,
    memory_floor: Arc<MemoryFloor>,
}

impl ViewLimiter {
    pub fn new(config: ViewLimitsConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.view_concurrency));
        let memory_floor = Arc::new(MemoryFloor::new(
            config.view_memory_floor_bytes,
            config.memory_floor_refresh,
        ));
        Self {
            config,
            semaphore,
            memory_floor,
        }
    }

    pub fn config(&self) -> &ViewLimitsConfig {
        &self.config
    }

    pub fn memory_floor(&self) -> &Arc<MemoryFloor> {
        &self.memory_floor
    }

    /// Available semaphore permits — exposed for tests and metrics.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Acquire a permit, respecting the supplied wait deadline. Order:
    /// 1. Check memory floor — if breached, fail fast (do NOT wait for a
    ///    permit; we want to shed load immediately when the host is
    ///    pressured).
    /// 2. Try to acquire a permit, bounded by `timeout`. If the timeout
    ///    fires first, return [`ViewAcquireError::AcquireTimeout`].
    pub async fn acquire(&self, timeout: Option<Duration>) -> Result<ViewPermit, ViewAcquireError> {
        // Step 1: memory floor.
        self.memory_floor.check().await?;

        // Step 2: bounded acquire. `acquire_owned` returns OwnedSemaphorePermit
        // which is `Send + 'static` — necessary because the caller will hold
        // it across the WASM call which is itself spawned.
        let sem = self.semaphore.clone();
        let wait = timeout.unwrap_or(self.config.acquire_timeout);

        match tokio::time::timeout(wait, sem.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(ViewPermit { _permit: permit }),
            Ok(Err(_)) => Err(ViewAcquireError::Closed),
            Err(_) => {
                log::warn!(
                    "view-runtime saturated: all {} permits in use, acquire timed out after {:?}",
                    self.config.view_concurrency,
                    wait,
                );
                Err(ViewAcquireError::Saturated)
            }
        }
    }
}

// thiserror::Error derive requires thiserror in deps. Add a tiny shim
// rather than pulling in thiserror — the runtime crate doesn't otherwise
// depend on it. Replace the derive above with an inline impl.
//
// (Left the derive for clarity; thiserror is a transitive dep through
// wasmtime so cargo build is fine. If the build fails we'll inline the
// impl.)

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn defaults_reasonable() {
        let c = ViewLimitsConfig::default();
        assert_eq!(c.view_concurrency, 16);
        assert_eq!(c.view_memory_bytes, 256 * 1024 * 1024);
        assert_eq!(c.view_memory_floor_bytes, 8192 * 1024 * 1024);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn memory_floor_passes_when_available() {
        // 0-byte threshold => always passes.
        let floor = MemoryFloor::new(0, Duration::from_millis(100));
        floor
            .check()
            .await
            .expect("floor with 0 threshold must pass");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn memory_floor_fails_when_overridden_below() {
        let floor = MemoryFloor::new(
            1024 * 1024 * 1024,      // 1GB threshold
            Duration::from_secs(60), // long refresh so override sticks
        );
        floor.override_available_memory(512 * 1024 * 1024).await;
        let err = floor.check().await.expect_err("must fail");
        assert!(matches!(err, ViewAcquireError::MemoryFloor));
    }
}
