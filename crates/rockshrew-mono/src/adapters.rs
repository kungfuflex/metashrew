use anyhow::Result;
use async_trait::async_trait;
use hex;
use metashrew_runtime::{KeyValueStoreLike, MetashrewRuntime};
use metashrew_sync::{
    AtomicBlockResult, BitcoinNodeAdapter, BlockInfo, ChainTip, PreviewCall, RuntimeAdapter,
    RuntimeStats, SyncError, SyncResult, ViewCall, ViewResult,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::ssh_tunnel::{make_request_with_tunnel, SshTunnel, SshTunnelConfig};

/// Map a view-runtime error into the SyncError taxonomy.
///
/// `wasmtime` surfaces a memory-cap trap with the substring
/// "memory growth has been disallowed" (StoreLimits trap_on_grow_failure)
/// or "out of memory" depending on the trap source. We sniff the chain and
/// promote those to [`SyncError::ResourceExhausted`] so the JSON-RPC layer
/// can return a -32002 error instead of a generic ViewFunction failure.
fn classify_view_error(err: anyhow::Error) -> SyncError {
    let chain = format!("{err:#}");
    let lower = chain.to_lowercase();
    if lower.contains("memory growth")
        || lower.contains("memory.grow")
        || lower.contains("out of memory")
        || lower.contains("memory size")
        || lower.contains("cannotgrow")
    {
        SyncError::ResourceExhausted(format!(
            "view function exceeded memory budget: {chain}"
        ))
    } else {
        SyncError::ViewFunction(format!("View function failed: {chain}"))
    }
}

// JSON-RPC request/response structs for BitcoinRpcAdapter
#[derive(Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub id: u32,
    pub jsonrpc: String,
    pub method: String,
    pub params: Vec<Value>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct BlockCountResponse {
    pub id: u32,
    pub result: Option<u32>,
    pub error: Option<Value>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct BlockHashResponse {
    pub id: u32,
    pub result: Option<String>,
    pub error: Option<Value>,
}

/// Environment escape hatch for [`verify_block_hash`].
///
/// Set `METASHREW_SKIP_BLOCK_HASH_VERIFY=1` for a chain whose block
/// identity is not `sha256d` over an 80-byte header.
const SKIP_VERIFY_ENV: &str = "METASHREW_SKIP_BLOCK_HASH_VERIFY";

/// Check that a block body actually hashes to the hash we asked for.
///
/// The node is told "give me the block at height H"; it answers with a
/// hash and then a body. Nothing previously connected the two — the
/// indexer took both on trust. Since the body is already in hand, the
/// hash is a double-SHA256 of its first 80 bytes away, so checking is
/// free and strictly stronger than asking.
///
/// This is the cheapest possible piece of the SPV argument: it does not
/// prove the chain, but it does mean a body cannot be silently swapped
/// for one at a different height, which is precisely the substitution a
/// remote or proxied node is in a position to make.
///
/// Block *identity* is `sha256d(header)` on essentially every
/// bitcoin-derived chain even where proof-of-work is not (litecoin's
/// scrypt, auxpow chains' merged mining) — auxpow data trails the
/// 80-byte header rather than replacing it. A chain that departs from
/// that can opt out via [`SKIP_VERIFY_ENV`]; the error names the
/// variable so the failure is self-explaining rather than mysterious.
pub fn verify_block_hash(height: u32, expected: &[u8], data: &[u8]) -> SyncResult<()> {
    if std::env::var(SKIP_VERIFY_ENV).is_ok() {
        return Ok(());
    }
    if data.len() < 80 {
        return Err(SyncError::BitcoinNode(format!(
            "block {} body is {} bytes, too short to contain a header",
            height,
            data.len()
        )));
    }
    let computed = double_sha256(&data[..80]);
    if computed.as_slice() != expected {
        return Err(SyncError::BitcoinNode(format!(
            "block {} body does not match the hash the node gave for that height \
             (expected {}, body hashes to {}). If this chain does not use \
             sha256d over an 80-byte header, set {}=1.",
            height,
            hex::encode(expected),
            hex::encode(computed),
            SKIP_VERIFY_ENV,
        )));
    }
    Ok(())
}

/// `sha256d`, in the same internal byte order bitcoind's `getblockhash`
/// returns (i.e. not reversed for display).
fn double_sha256(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let first = Sha256::digest(bytes);
    let second = Sha256::digest(first);
    let mut out = second.to_vec();
    out.reverse();
    out
}

/// Bitcoin node adapter that connects to a real Bitcoin node via RPC.
#[derive(Clone)]
pub struct BitcoinRpcAdapter {
    rpc_url: String,
    auth: Option<String>,
    bypass_ssl: bool,
    tunnel_config: Option<SshTunnelConfig>,
    active_tunnel: Arc<tokio::sync::Mutex<Option<SshTunnel>>>,
}

impl BitcoinRpcAdapter {
    pub fn new(
        rpc_url: String,
        auth: Option<String>,
        bypass_ssl: bool,
        tunnel_config: Option<SshTunnelConfig>,
    ) -> Self {
        Self {
            rpc_url,
            auth,
            bypass_ssl,
            tunnel_config,
            active_tunnel: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Fetch a block body by hash, skipping the height->hash lookup the
    /// caller has already done.
    async fn get_block_data_by_hash(&self, blockhash: &[u8]) -> SyncResult<Vec<u8>> {
        let params = vec![
            Value::String(hex::encode(blockhash)),
            Value::Number(Number::from(0)),
        ];
        let response: BlockHashResponse = self
            .request_with_retry("getblock", params)
            .await
            .map_err(|e| SyncError::BitcoinNode(e.to_string()))?;
        let block_hex = response
            .result
            .ok_or_else(|| SyncError::BitcoinNode("missing result".to_string()))?;
        hex::decode(block_hex)
            .map_err(|e| SyncError::BitcoinNode(format!("Hex decode error: {}", e)))
    }

    async fn request_with_retry<T: DeserializeOwned>(&self, method: &str, params: Vec<Value>) -> Result<T> {
        let max_retries = 5;
        let mut retry_delay = Duration::from_millis(500);
        let max_delay = Duration::from_secs(16);

        let mut active_tunnel_guard = if self.tunnel_config.is_some() {
            Some(self.active_tunnel.lock().await)
        } else {
            None
        };

        for attempt in 0..max_retries {
            let request_body = serde_json::to_string(&JsonRpcRequest {
                id: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| anyhow::anyhow!("Time error: {}", e))?
                    .as_secs() as u32,
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params: params.clone(),
            })
            .map_err(|e| anyhow::anyhow!("JSON serialization error: {}", e))?;

            let existing_tunnel: Option<SshTunnel> = if let Some(guard) = &active_tunnel_guard {
                (**guard).clone()
            } else {
                None
            };

            match make_request_with_tunnel(
                &self.rpc_url,
                request_body.clone(),
                self.auth.clone(),
                self.tunnel_config.clone(),
                self.bypass_ssl,
                existing_tunnel,
            )
            .await
            {
                Ok(tunneled_response) => {
                    if let Some(guard) = &mut active_tunnel_guard {
                        if guard.is_none() {
                            if let Some(tunnel) = tunneled_response._tunnel.clone() {
                                **guard = Some(tunnel);
                            }
                        }
                    }
                    
                    match tunneled_response.json::<T>().await {
                        Ok(response) => return Ok(response),
                        Err(e) => {
                            log::warn!("JSON parsing failed (attempt {}): {}. Retrying in {:?}...", attempt + 1, e, retry_delay);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Request failed (attempt {}): {}. Retrying in {:?}...", attempt + 1, e, retry_delay);
                    if let Some(guard) = &mut active_tunnel_guard {
                        **guard = None;
                    }
                }
            }

            // CRITICAL: Use deterministic jitter based on method name and attempt number
            // This ensures reproducible retry timing across different instances
            // Random jitter would cause non-deterministic block fetching patterns
            let jitter = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};

                let mut hasher = DefaultHasher::new();
                method.hash(&mut hasher);
                attempt.hash(&mut hasher);
                let hash = hasher.finish();
                (hash % 101) as u64 // 0-100 milliseconds, deterministic for same method+attempt
            };
            retry_delay = std::cmp::min(max_delay, retry_delay * 2 + Duration::from_millis(jitter));
            tokio::time::sleep(retry_delay).await;
        }

        Err(anyhow::anyhow!("Max retries exceeded for method {}", method))
    }
}
#[async_trait]
impl BitcoinNodeAdapter for BitcoinRpcAdapter {
    async fn get_tip_height(&self) -> SyncResult<u32> {
        let response: BlockCountResponse = self
            .request_with_retry("getblockcount", vec![])
            .await
            .map_err(|e| SyncError::BitcoinNode(e.to_string()))?;
        response
            .result
            .ok_or_else(|| SyncError::BitcoinNode("missing result".to_string()))
    }

    async fn get_block_hash(&self, height: u32) -> SyncResult<Vec<u8>> {
        let params = vec![Value::Number(Number::from(height))];
        let response: BlockHashResponse = self
            .request_with_retry("getblockhash", params)
            .await
            .map_err(|e| SyncError::BitcoinNode(e.to_string()))?;
        let blockhash = response
            .result
            .ok_or_else(|| SyncError::BitcoinNode("missing result".to_string()))?;
        hex::decode(blockhash)
            .map_err(|e| SyncError::BitcoinNode(format!("Hex decode error: {}", e)))
    }

    async fn get_block_data(&self, height: u32) -> SyncResult<Vec<u8>> {
        let blockhash = self.get_block_hash(height).await?;
        self.get_block_data_by_hash(&blockhash).await
    }

    async fn get_block_info(&self, height: u32) -> SyncResult<BlockInfo> {
        // One `getblockhash`, not two.
        //
        // This used to call `get_block_hash` and then `get_block_data`,
        // which called `get_block_hash` again for the same height — so a
        // third of every block's RPC traffic was a verbatim repeat of the
        // call made one line earlier. On a remote node that is a whole
        // round-trip of latency per block, for nothing.
        let hash = self.get_block_hash(height).await?;
        let data = self.get_block_data_by_hash(&hash).await?;
        verify_block_hash(height, &hash, &data)?;
        Ok(BlockInfo { height, hash, data })
    }

    async fn get_chain_tip(&self) -> SyncResult<ChainTip> {
        let height = self.get_tip_height().await?;
        let hash = self.get_block_hash(height).await?;
        Ok(ChainTip { height, hash })
    }

    async fn is_connected(&self) -> bool {
        self.get_tip_height().await.is_ok()
    }
}

/// MetashrewRuntime adapter that wraps the actual MetashrewRuntime and is snapshot-aware.
///
/// No external locking is needed for the runtime because:
/// - View/preview calls create independent WASM runtime instances
/// - Block processing is sequential (one block at a time)
/// - Database is append-only with height-based reads (concurrent reads are safe)
/// - MetashrewRuntime handles internal synchronization for context/instance access
///
/// v9.0.5-rc.2 view-runtime isolation: the adapter also holds an optional
/// per-view [`metashrew_runtime::ViewLimitsConfig`]. When set, `execute_view`
/// builds a fresh `StoreLimits` from the config and threads it through
/// `view_with_limits` so every view store is memory-capped. The indexer
/// path (`process_block` / `process_block_atomic`) does NOT consult the
/// config — block-application stays unbounded.
#[derive(Clone)]
pub struct MetashrewRuntimeAdapter<T: KeyValueStoreLike + Clone + Send + Sync + 'static> {
    runtime: Arc<MetashrewRuntime<T>>,
    snapshot_manager: Arc<RwLock<Option<Arc<RwLock<crate::snapshot::SnapshotManager>>>>>,
    view_limits: Option<Arc<metashrew_runtime::ViewLimitsConfig>>,
}

impl<T: KeyValueStoreLike + Clone + Send + Sync + 'static> MetashrewRuntimeAdapter<T> {
    pub fn new(runtime: Arc<MetashrewRuntime<T>>) -> Self {
        Self {
            runtime,
            snapshot_manager: Arc::new(RwLock::new(None)),
            view_limits: None,
        }
    }

    pub async fn set_snapshot_manager(&self, manager: Arc<RwLock<crate::snapshot::SnapshotManager>>) {
        let mut snapshot_manager = self.snapshot_manager.write().await;
        *snapshot_manager = Some(manager);
    }

    pub async fn get_snapshot_manager(&self) -> Option<Arc<RwLock<crate::snapshot::SnapshotManager>>> {
        self.snapshot_manager.read().await.as_ref().cloned()
    }

    /// Install a view-runtime limits config. Every subsequent
    /// `execute_view` call will be constrained by the per-view memory cap
    /// in `cfg.view_store_limits()`. Pass `None` to disable.
    pub fn with_view_limits(mut self, cfg: Arc<metashrew_runtime::ViewLimitsConfig>) -> Self {
        self.view_limits = Some(cfg);
        self
    }
}

#[async_trait]
impl<T: KeyValueStoreLike + Clone + Send + Sync + 'static> RuntimeAdapter for MetashrewRuntimeAdapter<T>
where
    <T as KeyValueStoreLike>::Batch: Send,
{
    async fn process_block(&self, height: u32, block_data: &[u8]) -> SyncResult<()> {
        // No external lock needed - MetashrewRuntime handles internal synchronization
        self.runtime.process_block(height, block_data).await.map_err(|e| SyncError::Runtime(e.to_string()))
    }
    async fn process_block_atomic(
        &self,
        height: u32,
        block_data: &[u8],
        block_hash: &[u8],
    ) -> SyncResult<AtomicBlockResult> {
        // No external lock needed - MetashrewRuntime handles internal synchronization
        self.runtime.process_block_atomic(height, block_data, block_hash).await.map(|r| AtomicBlockResult {
            batch_data: r.batch_data,
            height: r.height,
            block_hash: r.block_hash,
        }).map_err(|e| SyncError::Runtime(e.to_string()))
    }

    async fn execute_view(&self, call: ViewCall) -> SyncResult<ViewResult> {
        // view() creates a completely independent WASM runtime instance
        // No external locking needed - the method handles internal coordination
        // Database reads are safe due to append-only structure and height-based queries
        //
        // v9.0.5-rc.2: if a ViewLimitsConfig is installed, thread its
        // StoreLimits through so the view store is memory-capped. A WASM
        // memory.grow that hits the cap traps; we map that to
        // SyncError::ResourceExhausted so the JSON-RPC handler can surface
        // it as a -32002 error.
        let limits = self.view_limits.as_ref().map(|c| c.view_store_limits());
        let result = self
            .runtime
            .view_with_limits(call.function_name, &call.input_data, call.height, limits)
            .await
            .map_err(|e| classify_view_error(e))?;
        Ok(ViewResult { data: result })
    }

    async fn execute_preview(&self, call: PreviewCall) -> SyncResult<ViewResult> {
        // preview() creates an isolated DB copy, processes the block, then runs a view function
        // No external locking needed - all isolation is handled internally
        let result = self.runtime
            .preview_async(
                &call.block_data,
                call.function_name,
                &call.input_data,
                call.height,
            )
            .await
            .map_err(|e| SyncError::ViewFunction(format!("Preview function failed: {}", e)))?;
        Ok(ViewResult { data: result })
    }

    async fn refresh_memory(&self) -> SyncResult<()> {
        Ok(())
    }

    async fn is_ready(&self) -> bool {
        // No try_read needed - runtime is always ready for concurrent access
        true
    }

    async fn get_stats(&self) -> SyncResult<RuntimeStats> {
        // Briefly lock to get height, then release
        let blocks_processed = {
            let context = self.runtime.context.read().unwrap();
            context.height
        };
        Ok(RuntimeStats {
            memory_usage_bytes: 0,
            blocks_processed,
            last_refresh_height: Some(blocks_processed),
        })
    }
}
#[cfg(test)]
mod block_hash_tests {
    use super::*;

    /// Real signet block 1: the first 80 bytes of its body, and the hash
    /// bitcoind reports for height 1.
    const SIGNET_BLOCK_1_HEADER: &str = "00000020f61eee3b63a380a477a063af32b2bbc97c9ff9f01f2c\
4225e973988108000000f575c83235984e7dc4afc1f30944c170462e84437ab6f2d52e16878a79e4678bd1914d5fae7\
7031eccf40700";
    const SIGNET_BLOCK_1_HASH: &str =
        "00000086d6b2636cb2a392d45edc4ec544a10024d30141c9adf4bfd9de533b53";

    fn header() -> Vec<u8> {
        hex::decode(SIGNET_BLOCK_1_HEADER.replace('\n', "")).unwrap()
    }

    #[test]
    fn a_real_block_body_verifies_against_its_hash() {
        let mut body = header();
        // Trailing transaction bytes must not affect the hash.
        body.extend_from_slice(&[0xab; 249]);
        let hash = hex::decode(SIGNET_BLOCK_1_HASH).unwrap();
        assert!(verify_block_hash(1, &hash, &body).is_ok());
    }

    #[test]
    fn a_body_from_a_different_block_is_rejected() {
        let mut body = header();
        body[4] ^= 0xff; // perturb prev_blockhash
        let hash = hex::decode(SIGNET_BLOCK_1_HASH).unwrap();
        let err = verify_block_hash(1, &hash, &body).unwrap_err().to_string();
        assert!(err.contains("does not match"), "{err}");
        assert!(err.contains(SKIP_VERIFY_ENV), "error must name the escape hatch: {err}");
    }

    #[test]
    fn a_truncated_body_is_rejected_rather_than_panicking() {
        let hash = hex::decode(SIGNET_BLOCK_1_HASH).unwrap();
        assert!(verify_block_hash(1, &hash, &[0u8; 79]).is_err());
        assert!(verify_block_hash(1, &hash, &[]).is_err());
    }

    #[test]
    fn double_sha256_matches_the_published_block_hash() {
        assert_eq!(hex::encode(double_sha256(&header())), SIGNET_BLOCK_1_HASH);
    }
}
