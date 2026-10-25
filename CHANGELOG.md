# Changelog

Release notes for older versions are on the GitHub releases page
(https://github.com/kungfuflex/metashrew/releases).

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
