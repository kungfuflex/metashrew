# Security policy

## Reporting a vulnerability

**Do not open a public issue, pull request or discussion for a security
problem.**

Report it privately through GitHub's private vulnerability reporting:

1. Open the repository's **Security** tab.
2. Click **Report a vulnerability**
   (direct link: <https://github.com/kungfuflex/metashrew/security/advisories/new>).
3. Describe the issue, the affected version or commit, and how to reproduce
   it. A proof of concept is welcome but not required.

The report is visible only to you and the maintainers. We will acknowledge it,
keep you updated while we investigate, and coordinate a fix and disclosure
date with you. Please give us a reasonable chance to ship a fix before you
disclose anything publicly; credit is given in the advisory unless you ask
otherwise.

## Scope

In scope: the consensus and indexer code in this repository, in particular

- `rockshrew-mono`: the production indexer binary (block fetching and
  validation, reorg handling, its JSON-RPC server);
- `metashrew-runtime`: the wasmtime host that runs indexer WebAssembly,
  its determinism guarantees and its host functions;
- `metashrew-sync`, `rockshrew-runtime` and `memshrew-runtime`: block
  application, storage, rollback and snapshots;
- `metashrew-core` and `metashrew-support`: the guest-side libraries that are
  compiled into indexer WebAssembly modules;
- `metashrew-validator-core`: the chain-validation policies.

Examples of what we want to hear about: anything that makes two honest
indexers compute different state from the same chain, that corrupts or rolls
back committed state, that lets a remote party crash or wedge the indexer, or
that lets indexer WebAssembly escape the runtime.

Out of scope: indexer modules and protocols built by other projects on top of
metashrew (report those to their own maintainers), deployments operated by
third parties, the non-default `ssh-tunnel` cargo feature of `rockshrew-mono`
beyond what is already listed in `deny.toml`, and findings that only restate
an advisory already tracked there.

## Supported versions

Fixes go into the latest release line on `master`. Older release candidates
are not patched; upgrade to the newest release.
