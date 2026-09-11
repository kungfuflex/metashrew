//! Host runner for `--spv-type custom:<path>.wasm`.
//!
//! # Why a sandbox is enough here
//!
//! A validator is a pure function: it gets one block and its parent's
//! hash, and answers accept or reject. It has no database handle, no
//! network, and no way to influence anything except its own verdict. So
//! the module is instantiated with **exactly two imports** — read the
//! request length, copy the request in — and nothing else. Unknown
//! imports are defined as traps, so a module that reaches for anything
//! more fails to run rather than silently getting it.
//!
//! The worst a hostile custom validator can do is return a wrong
//! verdict, which is no worse than the operator having picked
//! `continuity`. It cannot exfiltrate, persist, or corrupt.
//!
//! # Why the built-ins do not go through here
//!
//! Checking a hash costs nanoseconds; instantiating a wasm module costs
//! microseconds to milliseconds. Running `mainnet` through a wasm
//! runtime would make the safety check the most expensive part of block
//! processing. Built-ins are plain Rust — this path exists only for
//! policies we do not ship.

use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use wasmtime::{Caller, Engine, Instance, Linker, Module, Store};

use crate::wire::{ValidationRequest, Verdict};
use crate::ChainValidator;

/// Per-instance state: the encoded request the guest is allowed to read.
struct ValidatorState {
    request: Vec<u8>,
}

/// A validation policy loaded from a wasm module.
pub struct WasmValidator {
    engine: Engine,
    module: Module,
    label: String,
    /// Serialises access. Validation is called once per block from the
    /// sync loop, so contention is not a concern and a mutex is simpler
    /// than a pool.
    lock: Mutex<()>,
}

impl WasmValidator {
    pub fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading validator {}", path.display()))?;
        Self::from_bytes(&bytes, path.display().to_string())
    }

    pub fn from_bytes(bytes: &[u8], label: String) -> Result<Self> {
        // Deterministic configuration, matching the indexer engine: a
        // validator that answered differently on different hosts would
        // be worse than no validator.
        let mut config = wasmtime::Config::default();
        config.cranelift_nan_canonicalization(true);
        config.relaxed_simd_deterministic(true);
        // No async: validation is a synchronous call from the sync path.
        let engine = Engine::new(&config).map_err(|e| anyhow!("building validator engine: {e}"))?;
        let module = Module::new(&engine, bytes).map_err(|e| anyhow!("compiling validator module: {e}"))?;

        let v = WasmValidator { engine, module, label, lock: Mutex::new(()) };
        // Fail at load rather than on the first block.
        v.run(&ValidationRequest {
            height: 0,
            header: vec![0u8; 80],
            claimed_hash: [0u8; 32],
            prev_hash: None,
        })
        .context("validator module did not run; expected an exported `validate() -> i32`")?;
        Ok(v)
    }

    fn run(&self, req: &ValidationRequest) -> Result<Verdict> {
        let _guard = self.lock.lock().unwrap_or_else(|p| p.into_inner());

        let mut store = Store::new(&self.engine, ValidatorState { request: req.encode() });
        let mut linker = Linker::<ValidatorState>::new(&self.engine);

        // Import 1: how many bytes is my request?
        linker
            .func_wrap("env", "__request_len", |caller: Caller<'_, ValidatorState>| {
                caller.data().request.len() as i32
            })
            .map_err(|e| anyhow!("defining __request_len: {e}"))?;

        // Import 2: copy it to `ptr`. Bounds-checked against the guest's
        // own memory — a bad pointer is the guest's bug, not a host crash.
        linker.func_wrap(
            "env",
            "__load_request",
            |mut caller: Caller<'_, ValidatorState>, ptr: i32| {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return;
                };
                let request = caller.data().request.clone();
                let _ = mem.write(&mut caller, ptr as usize, &request);
            },
        )
        .map_err(|e| anyhow!("defining __load_request: {e}"))?;

        // Anything else the module reaches for traps instead of resolving.
        linker
            .define_unknown_imports_as_traps(&self.module)
            .map_err(|e| anyhow!("defining unknown validator imports as traps: {e}"))?;

        let instance: Instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| anyhow!("instantiating validator: {e}"))?;

        let validate = instance
            .get_typed_func::<(), i32>(&mut store, "validate")
            .map_err(|e| anyhow!("validator has no `validate() -> i32` export: {e}"))?;
        let ptr = validate
            .call(&mut store, ())
            .map_err(|e| anyhow!("validator trapped: {e}"))?;

        let mem = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow!("validator exports no memory"))?;

        // The guest returns a pointer to `[len:u32 LE | payload]`.
        let data = mem.data(&store);
        let start = ptr as usize;
        if start < 4 || start > data.len() {
            return Err(anyhow!("validator returned an out-of-range pointer {ptr}"));
        }
        let len = u32::from_le_bytes(
            data.get(start - 4..start)
                .ok_or_else(|| anyhow!("validator response has no length prefix"))?
                .try_into()?,
        ) as usize;
        let payload = data
            .get(start..start + len)
            .ok_or_else(|| anyhow!("validator response length {len} runs past memory"))?;

        Verdict::decode(payload)
            .ok_or_else(|| anyhow!("validator returned a malformed verdict"))
    }
}

impl ChainValidator for WasmValidator {
    fn validate(&self, req: &ValidationRequest) -> Verdict {
        match self.run(req) {
            Ok(v) => v,
            // A validator that cannot run must not be treated as
            // approval. Fail closed.
            Err(e) => Verdict::reject(format!(
                "custom validator {} failed to produce a verdict: {e:#}",
                self.label
            )),
        }
    }

    fn describe(&self) -> String {
        format!("custom wasm validator: {}", self.label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A validator in WAT that reads its request and accepts iff the
    /// height is even — arbitrary, but it proves the request actually
    /// reaches the guest and the verdict comes back.
    const ACCEPT_EVEN_HEIGHTS: &str = r#"
(module
  (import "env" "__request_len" (func $req_len (result i32)))
  (import "env" "__load_request" (func $load_req (param i32)))
  (memory (export "memory") 2)

  (func $validate (export "validate") (result i32)
    (local $height i32)
    ;; Load the request at offset 256.
    (call $load_req (i32.const 256))
    ;; height is at request[1..5], little-endian.
    (local.set $height (i32.load (i32.const 257)))

    (if (i32.rem_u (local.get $height) (i32.const 2))
      (then
        ;; odd -> reject, reason "odd"
        (i32.store (i32.const 60) (i32.const 9))   ;; len prefix = 1+1+4+3
        (i32.store8 (i32.const 64) (i32.const 1))  ;; wire version
        (i32.store8 (i32.const 65) (i32.const 0))  ;; accepted = 0
        (i32.store (i32.const 66) (i32.const 3))   ;; reason_len = 3
        (i32.store8 (i32.const 70) (i32.const 111)) ;; 'o'
        (i32.store8 (i32.const 71) (i32.const 100)) ;; 'd'
        (i32.store8 (i32.const 72) (i32.const 100)) ;; 'd'
      )
      (else
        (i32.store (i32.const 60) (i32.const 6))   ;; len prefix
        (i32.store8 (i32.const 64) (i32.const 1))  ;; wire version
        (i32.store8 (i32.const 65) (i32.const 1))  ;; accepted = 1
        (i32.store (i32.const 66) (i32.const 0))   ;; reason_len = 0
      )
    )
    (i32.const 64)
  )
)
"#;

    fn req(height: u32) -> ValidationRequest {
        ValidationRequest {
            height,
            header: vec![0u8; 80],
            claimed_hash: [0u8; 32],
            prev_hash: None,
        }
    }

    fn build(wat: &str) -> Result<WasmValidator> {
        let bytes = wat::parse_str(wat)?;
        WasmValidator::from_bytes(&bytes, "test".to_string())
    }

    #[test]
    fn a_custom_validator_receives_the_request_and_returns_a_verdict() {
        let v = build(ACCEPT_EVEN_HEIGHTS).expect("load");
        assert!(v.validate(&req(0)).accepted);
        assert!(v.validate(&req(2)).accepted);

        let odd = v.validate(&req(7));
        assert!(!odd.accepted);
        assert_eq!(odd.reason, "odd");
    }

    #[test]
    fn a_module_without_the_export_is_rejected_at_load() {
        let err = match build("(module (memory (export \"memory\") 1))") {
            Ok(_) => panic!("a module without `validate` must not load"),
            Err(e) => e,
        };
        assert!(
            format!("{err:#}").contains("validate"),
            "error should name the missing export: {err:#}"
        );
    }

    /// A validator that traps must fail closed — never accept.
    #[test]
    fn a_trapping_validator_rejects_rather_than_accepts() {
        let wat = r#"
(module
  (import "env" "__request_len" (func $req_len (result i32)))
  (import "env" "__load_request" (func $load_req (param i32)))
  (memory (export "memory") 1)
  (func $validate (export "validate") (result i32) (unreachable))
)"#;
        // It traps during the load-time smoke run, so construction fails.
        assert!(build(wat).is_err(), "a trapping validator must not load");
    }

    /// The sandbox grants exactly two imports; anything else traps.
    #[test]
    fn a_validator_reaching_for_other_imports_cannot_run() {
        let wat = r#"
(module
  (import "env" "__request_len" (func $req_len (result i32)))
  (import "env" "__load_request" (func $load_req (param i32)))
  (import "env" "__get" (func $get (param i32 i32)))
  (memory (export "memory") 1)
  (func $validate (export "validate") (result i32)
    (call $get (i32.const 0) (i32.const 0))
    (i32.const 64))
)"#;
        // `define_unknown_imports_as_traps` lets it instantiate but the
        // call traps, so the smoke run fails and the module is refused.
        assert!(build(wat).is_err(), "reaching outside the sandbox must not load");
    }

    #[test]
    fn a_validator_returning_garbage_is_rejected_not_trusted() {
        let wat = r#"
(module
  (import "env" "__request_len" (func $req_len (result i32)))
  (import "env" "__load_request" (func $load_req (param i32)))
  (memory (export "memory") 1)
  (func $validate (export "validate") (result i32)
    ;; length prefix says 4 bytes, which is too short for a verdict
    (i32.store (i32.const 60) (i32.const 4))
    (i32.const 64))
)"#;
        assert!(build(wat).is_err(), "a malformed verdict must not be read as accept");
    }
}
