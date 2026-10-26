# Changelog

Release notes for older versions are on the GitHub releases page
(https://github.com/kungfuflex/metashrew/releases).

## v9.1.0-rc.6 (security)

A dependency-only security release. It supersedes **v9.1.0-rc.5**: that tag was signed and
pushed, but its tag pipeline failed `cargo deny` on four zstd advisories published after rc.5
was reviewed. The hourly GitHub mirror still copied the tag, but rc.5 was never released (no
GitHub Release). Use rc.6. **wasmtime moves to the 49.0.4
patch release** (below). The indexing, storage and reorg code doesn't change, and on x86_64
rc.6 indexes the same blocks into the same state as rc.4 and rc.5. It pairs with alkanes
**v3.0.0-rc.4**, whose indexer wasm is byte-identical to rc.3.

- **wasmtime 49.0.3 → 49.0.4 (RUSTSEC-2026-0378, RUSTSEC-2026-0379).** 0379 is a Cranelift
  bug on **aarch64**: a wasm `br_table` with more than about 262,000 targets could jump to the
  wrong target (ARM: Apple Silicon, Graviton, Ampere). wasmtime 49.0.4 fixes the code
  generation, and on **every architecture** it now **refuses to load a module with a
  `br_table` of more than 65,520 targets** (49.0.3 accepted them). So an indexer wasm that loads
  on rc.5 can fail to load on rc.6. Check any non-alkanes indexer you host against that limit.
  alkanes' indexer wasm (`27b003d1`) is well under both limits, so state indexed on aarch64
  with rc.5 or earlier is not affected and **needs no reindex**. Operators on aarch64 should
  still upgrade.
  0378 is a panic in component-model streams, which metashrew doesn't use (it runs core wasm
  modules only). Only the wasmtime, Cranelift and Pulley crates change version (patch
  bumps), and the enabled features are unchanged. Both `deny.toml` ignores are removed, and
  the regression ban now rejects wasmtime <= 49.0.3.

- **zstd (RUSTSEC-2026-0391, -0398, -0403, -0410).** `zstd` 0.13.3, `zstd-safe` 7.2.4 and
  `zstd-sys` 2.0.16 are affected, and the 0.13 line has no fix. They came in three ways:
  - **actix-web `compress-zstd` (removed).** No handler uses the Compress middleware, but with
    this feature actix-http decoded any request body sent with `Content-Encoding: zstd`
    through `zstd::stream::write::Decoder`, which is the path RUSTSEC-2026-0403 describes (a
    corrupt legacy frame exposes up to 32 KiB of uninitialized heap memory). That reached the
    JSON-RPC server of `rockshrew-mono` and the `memshrew-p2p` server from any client. Such
    request bodies are no longer decompressed: they reach the handler as sent and fail to
    parse. gzip and brotli request bodies still work. Responses are not compressed, as before.
  - **wasmtime `cache` (removed).** wasmtime's on-disk compiled-module cache, which metashrew
    never turns on (nothing calls `Config::cache`, and it is off unless configured). The
    workspace now declares wasmtime once (49.0.4, see above), with its default features minus `cache`.
    The resolved wasmtime and Cranelift feature sets are otherwise identical to rc.5.
  - **`rockshrew-mono` snapshot compression** (`zstd::encode_all` / `decode_all`) moves to
    `zstd` 0.14.3 / `zstd-safe` 8.2.0 / `zstd-sys` 2.1.2. Those bulk calls weren't affected
    by any of the four advisories. The bundled C library stays at zstd 1.5.7, so snapshots
    written by rc.5 read back unchanged. RocksDB's own zstd block compression
    (`librocksdb-sys`) links the same `zstd-sys` 2.1.2, with the same zstd 1.5.7.
  - RUSTSEC-2026-0410 is the wasm32 allocator shim in `zstd-sys`. metashrew never builds zstd
    for wasm32: the indexer wasm doesn't link zstd. It is fixed by the same bump.
- **cargo-deny:** new regression bans for the zstd versions above and for
  `wasmtime-internal-cache`. The existing bans now use `{ name, version }` ranges: cargo-deny
  0.20.2 drops the operator in the `crate = "name@<=x"` form and matches only that exact
  version, so the rc.1–rc.5 guards each banned only the one version they named. Checked
  against the rc.1 and rc.5 lockfiles, which they now reject (h2 0.3.27 and 0.4.13,
  actix-http 3.12.0, tokio 1.51.0, wasmtime 43.0.0, zstd 0.13.3 and its -safe/-sys pair).
- **Release:** all crates 9.1.0-rc.5 → 9.1.0-rc.6. `Cargo.lock` updates the three zstd
  crates and the wasmtime, Cranelift and Pulley crates (49.0.3 → 49.0.4, 0.136.3 → 0.136.4).
  It removes `wasmtime-internal-cache` and the crates that only it or `zstd-sys`
  2.0's bindgen pulled in (`bindgen` 0.72, `directories-next`, `dirs-sys-next`, `toml` 1.1
  and its `toml_*`/`serde_spanned`/`winnow` 1.0 dependencies), plus the workspace versions.

The known-and-not-fixed items listed under rc.5 (`russh` behind `ssh-tunnel`, `rsa`) are
unchanged.

## v9.1.0-rc.5 (security)

A dependency-only security release. **The runtime is unchanged:** wasmtime stays at
49.0.3, and the indexing, storage and reorg code doesn't change. rc.4 and rc.5 run the same
indexer wasm the same way. It pairs with alkanes **v3.0.0-rc.4**, whose indexer wasm is
byte-identical to rc.3.

- **TLS (CVE-2026-111943).** `rustls-webpki` 0.103.15 → 0.103.16 (RUSTSEC-2026-0382) and
  `rustls` 0.23.45 → 0.23.46 (RUSTSEC-2026-0346, -0347, -0348, -0349) in `Cargo.lock`. No
  metashrew binary compiles rustls today under any feature set. `reqwest` uses its
  `default-tls` (the operating system's TLS library and trust store), so this bump keeps
  `--locked` builds that turn rustls on clean. `rockshrew-mono` verifies TLS with the host's
  trust store. If you
  point it at an `https://` bitcoind or peer, keep the host's or image's `ca-certificates`
  package up to date. Rebuilding the image from this tag picks up the current package.
- **tokio** 1.51.0 → 1.51.6 (RUSTSEC-2026-0386, the 1.51 LTS patch).
- **cargo-deny:** regression bans for the versions above. RUSTSEC-2026-0378 and
  RUSTSEC-2026-0379 (wasmtime) are ignored with a reason: the first only affects
  component-model streams, which metashrew doesn't use, and the second only affects the
  aarch64 backend. Both are fixed by wasmtime 49.0.4, which will ship with the next runtime
  upgrade. Production x86_64 builds aren't affected.
- **Release:** all crates 9.1.0-rc.4 → 9.1.0-rc.5. `Cargo.lock` changes only the packages
  above and the workspace versions.

**Operators of older runtimes:** metashrew v9.1.0-rc.1 and earlier, the v9.0.5 line, and
the v10 alphas ship wasmtime 43.0.0, which is affected by CVE-2026-115384
(RUSTSEC-2026-0327). Move to v9.1.0-rc.3 or later. This release is the recommended
target.

Known and not fixed here: `russh` 0.50 (RUSTSEC-2026-0153/-0154) is only behind the
non-default `rockshrew-mono/ssh-tunnel` feature and needs a major upgrade (≥ 0.60.3).
`rsa` (RUSTSEC-2023-0071) has no fixed release.
