# Audit: what the "SPV-style" block validation actually guarantees

Scope: `metashrew-sync`'s `validate_block_connects`
(`sync.rs:437`, `snapshot_sync.rs:253`) and the block-body binding added
to `rockshrew-mono`'s `BitcoinRpcAdapter::get_block_info`.

The docstring calls this "Validate block chain continuity like a light
client (SPV-style)". That is accurate about *continuity* and misleading
about *SPV*. This document says precisely what is and is not enforced,
because the difference decides how much an operator may trust a remote
or proxied `--daemon-rpc-url`.

## What is enforced

Both sync engines call `validate_block_connects` unconditionally from
`process_block`, before the block is applied. It performs:

1. **Body ↔ hash binding.** `sha256d(header)` is recomputed from the
   block body and compared to the hash the node returned for that
   height. A body cannot be silently substituted for one at a different
   height.

2. **Parent linkage.** `header.prev_blockhash` is compared to the hash
   this node has already stored for `height - 1`. Blocks therefore form
   an unbroken chain in storage.

`rockshrew-mono` performs check 1 again at the fetch boundary
(`verify_block_hash`), so a mismatched body is rejected before it enters
the pipeline rather than after. That is deliberate duplication; if
either copy is changed, change both.

Together these give a real and useful property: **the stored chain is
internally consistent, and every block in it is the block its hash says
it is.**

## What is NOT enforced

Nothing in `metashrew-sync` validates any of the following. This was
confirmed by inspection (`grep` for target/difficulty/genesis/PoW across
the crate returns nothing) and by construction.

### 1. Proof of work — the load-bearing omission

No check that `sha256d(header) <= target(bits)`. Two linked headers with
an arbitrary nonce and a mainnet-looking `bits` field pass both checks
above while having no valid work at all:

```
fabricated block 1 hash: 94e2f0b7…604e273e
passes step 1 (hash binding):   True
passes step 2 (links to stored): True
PoW actually valid (hash <= target): False
```

Proof of work is the entire security assumption of SPV. Without it,
"following the correct chain" reduces to "trusting whatever the RPC
endpoint serves". The checks that *are* present only confirm the served
data is self-consistent — and the attacker supplies both sides of every
comparison.

### 2. Difficulty / retarget rules

`bits` is never validated against the retarget schedule, so even with
check 1 added an attacker could claim an arbitrarily easy target. A PoW
check alone raises the cost of forgery; only retarget validation bounds
it.

### 3. No genesis or checkpoint anchor

`height == 0` returns `Ok(true)` unconditionally — there is no
comparison against a known genesis hash for the network. Combined with
the fact that parent linkage is only ever checked against *our own
stored* hashes, which came from the same unauthenticated source, the
whole structure is ungrounded: an endpoint that serves a coherent
alternative chain from height 0 passes every check forever.

This is the single cheapest fix available, and it converts checks 1 and
2 from "self-consistent" into "self-consistent *and rooted in something
we independently know*".

### 4. No most-work fork choice

Reorg handling follows whatever the node reports. There is no cumulative
-work accounting, so the indexer cannot independently prefer the
heaviest branch; it inherits the node's opinion.

### 5. Linkage is skipped when the parent hash is missing

```rust
None => {
    warn!("No stored hash for block {} - unable to validate chain continuity …");
    Ok(true)   // allow processing to continue
}
```

Any gap in stored hashes — start of sync, restored snapshot, a height
whose hash was not persisted — silently disables check 2 for that block.
A warning is logged, but the block is accepted.

## Assessment

| property | status |
|---|---|
| body matches its claimed hash | enforced |
| blocks link into an unbroken chain | enforced, except when the parent hash is missing |
| headers carry valid proof of work | **not enforced** |
| difficulty follows consensus rules | **not enforced** |
| chain is rooted at the real genesis | **not enforced** |
| forks resolved by most work | **not enforced** |

**Conclusion: this is a hash-chain continuity check, not an SPV proof.**
It defends against a node that is buggy, out of sync, or serving
mismatched bodies. It does not defend against a node that is hostile,
compromised, or impersonated — which is exactly the threat model an SPV
light client exists to address, and exactly the model that matters when
`--daemon-rpc-url` points at a remote or proxied endpoint rather than
localhost.

Operators should treat the daemon RPC as **trusted infrastructure**
today. It should not be pointed at a third-party endpoint on the
assumption that these checks make that safe.

## Status

Genesis anchoring, proof of work, and the `None => Ok(true)` hole are
**closed** by `--enable-spv` (see `crates/metashrew-validator-core`).
Retarget validation and most-work fork choice remain **open**, and the
built-in policies say so in their startup banner rather than letting an
operator assume otherwise.

Measured cost of enforcement: signet 0..4351 took 18 s with SPV off and
19 s with `--spv-type signet`. Verified negatively as well — pointing
`--spv-type mainnet` at a signet endpoint rejects at height 0 naming
both hashes, and commits zero blocks.

## Recommended order of work

1. **Genesis / checkpoint anchor** (small, no consensus logic). A
   `--genesis-hash` compared at height 0, and optionally a small set of
   checkpoint heights. Grounds the existing linkage check.
2. **Proof-of-work check** (`hash <= target(bits)`), self-contained and
   valid on mainnet, testnet, signet and regtest alike — each sets
   `bits` to its own difficulty. **Must be opt-in or chain-aware**:
   merged-mining chains this tree supports (dogecoin, luckycoin,
   bellscoin) satisfy the target with the auxpow parent header, not the
   block header, so a blanket check would reject valid blocks.
3. **Retarget validation**, to bound the forgery cost implied by (2).
4. **Cumulative-work fork choice**, so reorgs are decided locally.

Items 1 and 2 together would move the guarantee from "the node did not
contradict itself" to "the chain is anchored and carries real work",
which is the point at which the SPV description becomes earned.

## Also fixed while auditing this path

`get_block_info` previously issued three RPCs per block, one a verbatim
duplicate `getblockhash`, and every RPC built a fresh `reqwest::Client`
(discarding its connection pool immediately, so each request paid a full
TLS handshake). Signet 0..4351 against a remote endpoint went from 912 s
to 26 s with 775 fetch retries reduced to 0. See the commit for detail.
