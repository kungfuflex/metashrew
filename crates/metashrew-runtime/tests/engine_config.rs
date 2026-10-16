//! Golden test for the wasmtime engine configuration.
//!
//! The indexer engine's behaviour must not change when wasmtime is
//! upgraded. wasmtime's defaults move between releases (46-49 turned on
//! GC, exceptions, function-references, wide-arithmetic and
//! component-model-async by default), so `indexer_config()` pins every
//! proposal flag. This test reads the *effective* flags from the
//! `Debug` output of the `Config` (wasmtime prints every `WasmFeatures`
//! flag there) and checks them against the values the production engine
//! had under wasmtime 43.0.0 (metashrew v9.1.0-rc.1), captured by
//! printing the same `Config` built against 43.0.0.
//!
//! A flag that did not exist in wasmtime 43 must be off. If a future
//! wasmtime adds or renames a flag, this test fails and the new flag has
//! to be reviewed and pinned before the upgrade lands.

use std::collections::BTreeMap;

/// Effective proposal flags of the indexer engine under wasmtime 43.0.0.
/// (`cm_async_builtins` was renamed `cm_more_async_builtins` later; both
/// are off.)
const WASMTIME_43_FLAGS: &[(&str, bool)] = &[
    ("wasm_mutable_global", true),
    ("wasm_saturating_float_to_int", true),
    ("wasm_sign_extension", true),
    ("wasm_reference_types", true),
    ("wasm_multi_value", true),
    ("wasm_bulk_memory", true),
    ("wasm_simd", true),
    ("wasm_relaxed_simd", true),
    ("wasm_threads", true),
    ("wasm_shared_everything_threads", false),
    ("wasm_tail_call", true),
    ("wasm_floats", true),
    ("wasm_multi_memory", true),
    ("wasm_exceptions", false),
    ("wasm_memory64", true),
    ("wasm_extended_const", true),
    ("wasm_component_model", true),
    ("wasm_function_references", false),
    ("wasm_memory_control", false),
    ("wasm_gc", false),
    ("wasm_custom_page_sizes", false),
    ("wasm_legacy_exceptions", false),
    ("wasm_gc_types", true),
    ("wasm_stack_switching", false),
    ("wasm_wide_arithmetic", false),
    ("wasm_cm_values", false),
    ("wasm_cm_nested_names", false),
    ("wasm_cm_async", false),
    ("wasm_cm_async_stackful", false),
    ("wasm_cm_async_builtins", false),
    ("wasm_cm_threading", false),
    ("wasm_cm_error_context", false),
    ("wasm_cm_fixed_length_lists", false),
    ("wasm_cm_gc", false),
    ("wasm_call_indirect_overlong", true),
    ("wasm_bulk_memory_opt", true),
    ("wasm_custom_descriptors", false),
    ("wasm_compact_imports", false),
    ("wasm_cm_map", false),
];

fn flags(config: &wasmtime::Config) -> BTreeMap<String, bool> {
    let dbg = format!("{:#?}", config);
    let mut out = BTreeMap::new();
    for line in dbg.lines() {
        let line = line.trim().trim_end_matches(',');
        if let Some((name, value)) = line.split_once(": ") {
            if name.starts_with("wasm_") && (value == "true" || value == "false") {
                out.insert(name.to_string(), value == "true");
            }
        }
    }
    out
}

fn check(config: &wasmtime::Config, what: &str) {
    let actual = flags(config);
    assert!(
        actual.len() >= 30,
        "{what}: could not read wasm feature flags from Config's Debug output:\n{:#?}",
        config
    );
    let expected: BTreeMap<&str, bool> = WASMTIME_43_FLAGS.iter().copied().collect();
    let mut errors = vec![];
    for (name, value) in &actual {
        match expected.get(name.as_str()) {
            Some(want) if want != value => {
                errors.push(format!("{name} = {value}, wasmtime 43 had {want}"))
            }
            None if *value => errors.push(format!(
                "{name} = true, but it did not exist (so was off) in wasmtime 43"
            )),
            _ => {}
        }
    }
    assert!(
        errors.is_empty(),
        "{what}: engine config drifted:\n  {}",
        errors.join("\n  ")
    );
}

#[test]
fn indexer_engine_flags_match_wasmtime_43() {
    check(
        &metashrew_runtime::runtime::indexer_config(),
        "indexer_config",
    );
}

#[test]
fn view_engine_flags_match_wasmtime_43() {
    check(&metashrew_runtime::runtime::view_config(), "view_config");
}

#[test]
fn deterministic_settings_are_present() {
    for (what, config) in [
        ("indexer", metashrew_runtime::runtime::indexer_config()),
        ("view", metashrew_runtime::runtime::view_config()),
    ] {
        let dbg = format!("{:?}", config);
        for needle in [
            "\"enable_nan_canonicalization\"",
            "relaxed_simd_deterministic: true",
            "memory_reservation: 4294967296",
            "memory_guard_size: 65536",
            "memory_init_cow: false",
            "Cranelift",
        ] {
            assert!(dbg.contains(needle), "{what}: missing {needle} in {dbg}");
        }
    }
    let view = format!("{:?}", metashrew_runtime::runtime::view_config());
    assert!(view.contains("consume_fuel: true"), "{view}");
    let indexer = format!("{:?}", metashrew_runtime::runtime::indexer_config());
    assert!(!indexer.contains("consume_fuel: true"), "{indexer}");
    // Both engines must build.
    wasmtime::Engine::new(&metashrew_runtime::runtime::indexer_config()).unwrap();
    wasmtime::Engine::new(&metashrew_runtime::runtime::view_config()).unwrap();
}
