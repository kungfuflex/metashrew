use std::env;
use std::process::Command;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    // Build the metashrew-minimal WASM.
    //
    // The nested cargo must not inherit the host build's flags: cargo sets
    // CARGO_ENCODED_RUSTFLAGS for build scripts, and tools like
    // cargo-llvm-cov put `-C instrument-coverage` there, which cannot link
    // for wasm32-unknown-unknown (no profiler runtime).
    let status = Command::new("cargo")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .arg("build")
        .arg("--release")
        .arg("--target=wasm32-unknown-unknown")
        .arg("-p")
        .arg("metashrew-minimal")
        .arg("--target-dir")
        .arg(format!("{}/wasm", out_dir))
        .status()
        .expect("Failed to build metashrew-minimal");

    if !status.success() {
        panic!("Failed to build metashrew-minimal WASM module");
    }

    let wasm_path = format!(
        "{}/wasm/wasm32-unknown-unknown/release/metashrew_minimal.wasm",
        out_dir
    );

    println!("cargo:rustc-env=METASHREW_MINIMAL_WASM_PATH={}", wasm_path);
    println!("cargo:rerun-if-changed=crates/metashrew-minimal/src/lib.rs");
}
