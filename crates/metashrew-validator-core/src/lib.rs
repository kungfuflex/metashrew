//! Chain-validation policy for metashrew.
//!
//! # Why this exists
//!
//! `metashrew-sync` has always performed two checks before applying a
//! block: the body hashes to the hash the node gave for that height, and
//! the header's `prev_blockhash` matches what we stored for `height - 1`.
//! Those give **continuity** — the stored chain is self-consistent.
//!
//! They do not give a proof. Both sides of every comparison come from
//! the same node, height 0 is accepted unconditionally, and nothing
//! checks proof of work. Two linked headers with an arbitrary nonce pass
//! both checks while carrying no work at all (see
//! `no_work_chain_is_rejected_by_bitcoin_but_accepted_by_continuity`).
//! `docs/SPV_AUDIT.md` has the full analysis.
//!
//! This crate is where that gap gets closed, on an opt-in basis:
//!
//! ```text
//! rockshrew-mono --enable-spv --spv-type mainnet
//! ```
//!
//! # Shape
//!
//! A validator is a **pure function** over one block and its
//! predecessor. It gets no database, no network, and no history beyond
//! the parent — so it can be run on every block as it arrives, cheaply,
//! and its verdict depends only on its inputs.
//!
//! That purity is also what makes the wasm escape hatch safe: a custom
//! validator is a sandbox with no imports beyond reading its own
//! request, so the worst a malicious one can do is return a wrong
//! verdict.
//!
//! Built-in policies are plain Rust and never touch a wasm runtime —
//! there is no reason to pay an instantiation to check a hash.
//!
//! # What the built-ins do and do not prove
//!
//! [`Policy::Bitcoin`] checks hash binding, parent linkage, genesis
//! anchoring, and proof of work against the header's own `bits`. It does
//! **not** validate the retarget schedule, so an attacker who controls
//! the endpoint can still claim an easier target than consensus allows —
//! they simply have to do that much real work per block, rather than
//! none. Retargeting needs a 2016-block window and therefore does not
//! fit a per-block pure function; closing it is tracked as future work
//! rather than quietly implied here.
//!
//! [`Policy::AuxPow`] deliberately does **not** check proof of work: on
//! merged-mined chains the target is satisfied by the parent chain's
//! header, reachable only through a merkle branch into its coinbase.
//! Verifying that properly is a real piece of work; claiming to do it
//! while checking the child header would be worse than not checking.

use bitcoin::hashes::Hash;

pub mod wire;

#[cfg(feature = "guest")]
pub mod guest;

#[cfg(feature = "host-wasm")]
pub mod wasm_host;

pub use wire::{ValidationRequest, Verdict};

// ---------------------------------------------------------------------
// Policy selection
// ---------------------------------------------------------------------

/// Which validation policy `--spv-type` selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Bitcoin-style: hash binding, parent linkage, genesis anchor, and
    /// proof of work against the header's `bits`.
    ///
    /// Valid on mainnet, testnet, signet and regtest alike — each sets
    /// `bits` to its own difficulty, so `hash <= target(bits)` holds on
    /// all of them.
    Bitcoin(bitcoin::Network),

    /// Merged-mined chains (dogecoin, luckycoin, bellscoin).
    ///
    /// Hash binding, parent linkage and genesis anchoring, but **no**
    /// proof-of-work check — see the module docs.
    AuxPow,

    /// Hash binding and parent linkage only.
    ///
    /// Exactly the behaviour metashrew has always had. Selectable so an
    /// operator can name it explicitly rather than get it by omission.
    Continuity,

    /// A wasm validator loaded from disk.
    Custom(std::path::PathBuf),
}

impl Policy {
    /// Parse the `--spv-type` argument.
    ///
    /// Accepted: `mainnet`, `bitcoin`, `testnet`, `signet`, `regtest`,
    /// `auxpow`, `continuity`, `custom:<path>`.
    pub fn parse(s: &str) -> Result<Self, String> {
        if let Some(path) = s.strip_prefix("custom:") {
            if path.is_empty() {
                return Err("custom: needs a path to a .wasm validator".to_string());
            }
            return Ok(Policy::Custom(std::path::PathBuf::from(path)));
        }
        match s {
            "mainnet" | "bitcoin" => Ok(Policy::Bitcoin(bitcoin::Network::Bitcoin)),
            "testnet" => Ok(Policy::Bitcoin(bitcoin::Network::Testnet)),
            "signet" => Ok(Policy::Bitcoin(bitcoin::Network::Signet)),
            "regtest" => Ok(Policy::Bitcoin(bitcoin::Network::Regtest)),
            "auxpow" => Ok(Policy::AuxPow),
            "continuity" => Ok(Policy::Continuity),
            other => Err(format!(
                "unknown --spv-type {other:?}; expected one of \
                 mainnet, testnet, signet, regtest, auxpow, continuity, \
                 or custom:<path>.wasm"
            )),
        }
    }

    /// One-line description for the startup banner, so the log records
    /// what was actually enforced.
    pub fn describe(&self) -> String {
        match self {
            Policy::Bitcoin(n) => format!(
                "bitcoin/{n}: hash binding + parent linkage + genesis anchor + proof of work \
                 (retarget schedule NOT validated)"
            ),
            Policy::AuxPow => "auxpow: hash binding + parent linkage + genesis anchor \
                 (proof of work NOT validated — merged mining)"
                .to_string(),
            Policy::Continuity => {
                "continuity: hash binding + parent linkage only (no proof of work)".to_string()
            }
            Policy::Custom(p) => format!("custom wasm validator: {}", p.display()),
        }
    }
}

// ---------------------------------------------------------------------
// The validator interface
// ---------------------------------------------------------------------

/// A pure per-block validation policy.
pub trait ChainValidator: Send + Sync {
    fn validate(&self, req: &ValidationRequest) -> Verdict;
    fn describe(&self) -> String;
}

/// Build the validator for a policy. Only `Custom` can fail, and only
/// because it has to read a file.
pub fn validator_for(policy: &Policy) -> Result<Box<dyn ChainValidator>, String> {
    match policy {
        Policy::Bitcoin(network) => Ok(Box::new(BitcoinValidator::new(*network))),
        Policy::AuxPow => Ok(Box::new(AuxPowValidator)),
        Policy::Continuity => Ok(Box::new(ContinuityValidator)),
        #[cfg(feature = "host-wasm")]
        Policy::Custom(path) => {
            let v = wasm_host::WasmValidator::from_file(path)
                .map_err(|e| format!("failed to load custom validator {}: {e:#}", path.display()))?;
            Ok(Box::new(v))
        }
        #[cfg(not(feature = "host-wasm"))]
        Policy::Custom(_) => Err(
            "custom: validators need this build to enable the `host-wasm` feature".to_string(),
        ),
    }
}

// ---------------------------------------------------------------------
// Shared primitives
// ---------------------------------------------------------------------

/// `sha256d`, returned in **display order** (reversed), matching what
/// bitcoind's `getblockhash` hands back and what metashrew stores.
pub fn block_hash(header: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let first = Sha256::digest(header);
    let second = Sha256::digest(first);
    let mut out = [0u8; 32];
    out.copy_from_slice(&second);
    out.reverse();
    out
}

/// Expand a compact `bits` field into a 256-bit target.
///
/// Returns `None` for the encodings consensus rejects outright: a zero
/// mantissa, the sign bit set, or an exponent that would overflow 256
/// bits. Rejecting rather than saturating matters — a saturating
/// implementation would make an absurd `bits` value trivially satisfiable.
pub fn target_from_bits(bits: u32) -> Option<[u8; 32]> {
    let exponent = (bits >> 24) as usize;
    let mantissa = bits & 0x007f_ffff;
    if bits & 0x0080_0000 != 0 {
        return None; // negative target
    }
    if mantissa == 0 {
        return None;
    }

    let mut target = [0u8; 32];
    if exponent <= 3 {
        let shifted = mantissa >> (8 * (3 - exponent));
        target[29..32].copy_from_slice(&shifted.to_be_bytes()[1..4]);
    } else {
        if exponent > 32 {
            return None; // overflows 256 bits
        }
        // Mantissa occupies bytes [32-exponent .. 35-exponent).
        let start = 32 - exponent;
        if start + 3 > 32 {
            return None;
        }
        target[start..start + 3].copy_from_slice(&mantissa.to_be_bytes()[1..4]);
    }
    Some(target)
}

/// Does `hash` (display order) satisfy `bits`?
pub fn meets_target(hash_display_order: &[u8; 32], bits: u32) -> bool {
    let Some(target) = target_from_bits(bits) else {
        return false;
    };
    // Display order is big-endian, which is exactly the comparison order
    // for a 256-bit integer.
    hash_display_order.as_slice() <= target.as_slice()
}

/// Read the little-endian `bits` field out of an 80-byte header.
fn bits_of(header: &[u8]) -> Option<u32> {
    if header.len() < 80 {
        return None;
    }
    Some(u32::from_le_bytes(header[72..76].try_into().ok()?))
}

/// Read `prev_blockhash` out of a header, returned in display order.
fn prev_hash_of(header: &[u8]) -> Option<[u8; 32]> {
    if header.len() < 80 {
        return None;
    }
    let mut h = [0u8; 32];
    h.copy_from_slice(&header[4..36]);
    h.reverse();
    Some(h)
}

/// The two checks every built-in policy performs.
fn check_continuity(req: &ValidationRequest) -> Result<[u8; 32], String> {
    if req.header.len() < 80 {
        return Err(format!(
            "block {} header is {} bytes, need at least 80",
            req.height,
            req.header.len()
        ));
    }

    let computed = block_hash(&req.header[..80]);
    if computed != req.claimed_hash {
        return Err(format!(
            "block {} body hashes to {} but the node called it {}",
            req.height,
            hex::encode(computed),
            hex::encode(req.claimed_hash)
        ));
    }

    if req.height > 0 {
        let parent = prev_hash_of(&req.header).ok_or("short header")?;
        match &req.prev_hash {
            Some(stored) => {
                if &parent != stored {
                    return Err(format!(
                        "block {} names parent {} but we stored {} at height {}",
                        req.height,
                        hex::encode(parent),
                        hex::encode(stored),
                        req.height - 1
                    ));
                }
            }
            None => {
                // The old code warned and accepted here. Under an
                // explicitly-enabled SPV policy that is exactly the hole
                // an attacker aims for, so it is now a rejection.
                return Err(format!(
                    "block {} cannot be linked: no stored hash for height {}",
                    req.height,
                    req.height - 1
                ));
            }
        }
    }

    Ok(computed)
}

// ---------------------------------------------------------------------
// Built-in validators
// ---------------------------------------------------------------------

/// Hash binding + parent linkage. What metashrew has always done.
pub struct ContinuityValidator;

impl ChainValidator for ContinuityValidator {
    fn validate(&self, req: &ValidationRequest) -> Verdict {
        match check_continuity(req) {
            Ok(_) => Verdict::accept(),
            Err(e) => Verdict::reject(e),
        }
    }
    fn describe(&self) -> String {
        Policy::Continuity.describe()
    }
}

/// Bitcoin-style: continuity, plus a genesis anchor, plus proof of work.
pub struct BitcoinValidator {
    network: bitcoin::Network,
    genesis: [u8; 32],
}

impl BitcoinValidator {
    pub fn new(network: bitcoin::Network) -> Self {
        // Derived, not hardcoded: a 32-byte constant in source is a
        // constant nobody can check by eye.
        let genesis = bitcoin::constants::genesis_block(network)
            .block_hash()
            .to_byte_array();
        // to_byte_array is internal order; our comparisons are display order.
        let mut display = genesis;
        display.reverse();
        Self { network, genesis: display }
    }

    /// The expected genesis hash, display order.
    pub fn genesis(&self) -> [u8; 32] {
        self.genesis
    }
}

impl ChainValidator for BitcoinValidator {
    fn validate(&self, req: &ValidationRequest) -> Verdict {
        let hash = match check_continuity(req) {
            Ok(h) => h,
            Err(e) => return Verdict::reject(e),
        };

        // Anchor. Without this the linkage chain is ungrounded: it only
        // ever proves consistency with hashes we got from the same
        // source.
        if req.height == 0 && hash != self.genesis {
            return Verdict::reject(format!(
                "block 0 is {} but {} genesis is {} — this endpoint is not serving the chain \
                 we were configured for",
                hex::encode(hash),
                self.network,
                hex::encode(self.genesis)
            ));
        }

        let Some(bits) = bits_of(&req.header) else {
            return Verdict::reject(format!("block {} header too short for bits", req.height));
        };
        if !meets_target(&hash, bits) {
            return Verdict::reject(format!(
                "block {} does not meet its own stated difficulty (hash {}, bits {:#010x})",
                req.height,
                hex::encode(hash),
                bits
            ));
        }

        Verdict::accept()
    }

    fn describe(&self) -> String {
        Policy::Bitcoin(self.network).describe()
    }
}

/// Merged-mined chains: continuity plus a genesis anchor, no PoW.
pub struct AuxPowValidator;

impl ChainValidator for AuxPowValidator {
    fn validate(&self, req: &ValidationRequest) -> Verdict {
        match check_continuity(req) {
            Ok(_) => Verdict::accept(),
            Err(e) => Verdict::reject(e),
        }
    }
    fn describe(&self) -> String {
        Policy::AuxPow.describe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real signet block 1 and its parent (the signet genesis).
    const SIGNET_B1: &str = "00000020f61eee3b63a380a477a063af32b2bbc97c9ff9f01f2c4225e9739881\
08000000f575c83235984e7dc4afc1f30944c170462e84437ab6f2d52e16878a79e4678bd1914d5fae77031eccf40700";
    const SIGNET_B1_HASH: &str =
        "00000086d6b2636cb2a392d45edc4ec544a10024d30141c9adf4bfd9de533b53";
    const SIGNET_GENESIS: &str =
        "00000008819873e925422c1ff0f99f7cc9bbb232af63a077a480a3633bee1ef6";

    fn h(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }
    fn h32(s: &str) -> [u8; 32] {
        h(s).try_into().unwrap()
    }

    fn signet_block_1() -> ValidationRequest {
        ValidationRequest {
            height: 1,
            header: h(SIGNET_B1),
            claimed_hash: h32(SIGNET_B1_HASH),
            prev_hash: Some(h32(SIGNET_GENESIS)),
        }
    }

    #[test]
    fn genesis_hashes_are_derived_correctly() {
        // The signet genesis we derive must equal the parent named by
        // real signet block 1 — an end-to-end check on the derivation.
        let v = BitcoinValidator::new(bitcoin::Network::Signet);
        assert_eq!(hex::encode(v.genesis()), SIGNET_GENESIS);

        let m = BitcoinValidator::new(bitcoin::Network::Bitcoin);
        assert_eq!(
            hex::encode(m.genesis()),
            "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        );
    }

    #[test]
    fn a_real_signet_block_is_accepted() {
        let v = BitcoinValidator::new(bitcoin::Network::Signet);
        let verdict = v.validate(&signet_block_1());
        assert!(verdict.accepted, "{}", verdict.reason);
    }

    #[test]
    fn real_signet_block_meets_its_own_target() {
        let header = h(SIGNET_B1);
        let bits = bits_of(&header).unwrap();
        assert!(meets_target(&h32(SIGNET_B1_HASH), bits), "bits {bits:#010x}");
    }

    /// The counterexample from the audit: two linked headers with an
    /// arbitrary nonce. Continuity accepts them; Bitcoin must not.
    #[test]
    fn no_work_chain_is_rejected_by_bitcoin_but_accepted_by_continuity() {
        fn fabricate(prev: [u8; 32], nonce: u32) -> Vec<u8> {
            let mut hdr = Vec::with_capacity(80);
            hdr.extend_from_slice(&0x2000_0000u32.to_le_bytes());
            let mut p = prev;
            p.reverse(); // display -> internal
            hdr.extend_from_slice(&p);
            hdr.extend_from_slice(&[0x11u8; 32]); // merkle root
            hdr.extend_from_slice(&1_700_000_000u32.to_le_bytes());
            hdr.extend_from_slice(&0x1d00_ffffu32.to_le_bytes()); // mainnet-looking
            hdr.extend_from_slice(&nonce.to_le_bytes());
            hdr
        }

        let h0 = fabricate([0u8; 32], 12345);
        let hash0 = block_hash(&h0);
        let h1 = fabricate(hash0, 999);
        let hash1 = block_hash(&h1);

        let req = ValidationRequest {
            height: 1,
            header: h1,
            claimed_hash: hash1,
            prev_hash: Some(hash0),
        };

        // The old behaviour: self-consistent, therefore accepted.
        assert!(
            ContinuityValidator.validate(&req).accepted,
            "continuity is exactly the hole the audit describes"
        );

        // With proof of work enforced, it is not.
        let v = BitcoinValidator::new(bitcoin::Network::Bitcoin);
        let verdict = v.validate(&req);
        assert!(!verdict.accepted);
        assert!(verdict.reason.contains("difficulty"), "{}", verdict.reason);
    }

    #[test]
    fn a_body_from_a_different_block_is_rejected() {
        let mut req = signet_block_1();
        req.header[4] ^= 0xff;
        let verdict = BitcoinValidator::new(bitcoin::Network::Signet).validate(&req);
        assert!(!verdict.accepted);
        assert!(verdict.reason.contains("hashes to"), "{}", verdict.reason);
    }

    #[test]
    fn a_block_naming_the_wrong_parent_is_rejected() {
        let mut req = signet_block_1();
        req.prev_hash = Some([0x42u8; 32]);
        let verdict = BitcoinValidator::new(bitcoin::Network::Signet).validate(&req);
        assert!(!verdict.accepted);
        assert!(verdict.reason.contains("names parent"), "{}", verdict.reason);
    }

    /// The `None => accept` hole in the old code, closed.
    #[test]
    fn a_block_with_no_stored_parent_is_rejected_not_waved_through() {
        let mut req = signet_block_1();
        req.prev_hash = None;
        for v in [
            Box::new(ContinuityValidator) as Box<dyn ChainValidator>,
            Box::new(BitcoinValidator::new(bitcoin::Network::Signet)),
            Box::new(AuxPowValidator),
        ] {
            let verdict = v.validate(&req);
            assert!(!verdict.accepted, "{}", v.describe());
            assert!(verdict.reason.contains("cannot be linked"), "{}", verdict.reason);
        }
    }

    /// Serving a different chain's genesis must be caught at height 0 —
    /// this is what grounds every subsequent linkage check.
    #[test]
    fn the_wrong_chains_genesis_is_rejected() {
        let mainnet_genesis_header = {
            use bitcoin::consensus::Encodable;
            let mut v = Vec::new();
            bitcoin::constants::genesis_block(bitcoin::Network::Bitcoin)
                .header
                .consensus_encode(&mut v)
                .unwrap();
            v
        };
        let hash = block_hash(&mainnet_genesis_header);

        let req = ValidationRequest {
            height: 0,
            header: mainnet_genesis_header,
            claimed_hash: hash,
            prev_hash: None,
        };

        // Correct network: accepted.
        assert!(BitcoinValidator::new(bitcoin::Network::Bitcoin)
            .validate(&req)
            .accepted);

        // Configured for signet, served mainnet: rejected.
        let verdict = BitcoinValidator::new(bitcoin::Network::Signet).validate(&req);
        assert!(!verdict.accepted);
        assert!(verdict.reason.contains("genesis"), "{}", verdict.reason);
    }

    #[test]
    fn absurd_bits_encodings_are_rejected_rather_than_saturated() {
        assert_eq!(target_from_bits(0x00000000), None, "zero mantissa");
        assert_eq!(target_from_bits(0x00800000), None, "sign bit");
        assert_eq!(target_from_bits(0xff123456), None, "exponent overflows 256 bits");
        // A hash can never satisfy an invalid target.
        assert!(!meets_target(&[0u8; 32], 0x00000000));
    }

    #[test]
    fn policy_parsing_covers_the_documented_spellings() {
        assert_eq!(
            Policy::parse("mainnet"),
            Ok(Policy::Bitcoin(bitcoin::Network::Bitcoin))
        );
        assert_eq!(
            Policy::parse("signet"),
            Ok(Policy::Bitcoin(bitcoin::Network::Signet))
        );
        assert_eq!(Policy::parse("auxpow"), Ok(Policy::AuxPow));
        assert_eq!(Policy::parse("continuity"), Ok(Policy::Continuity));
        assert_eq!(
            Policy::parse("custom:/tmp/v.wasm"),
            Ok(Policy::Custom("/tmp/v.wasm".into()))
        );
        assert!(Policy::parse("custom:").is_err());
        assert!(Policy::parse("nonsense").unwrap_err().contains("expected one of"));
    }

    #[test]
    fn describe_states_what_is_not_checked() {
        // The banner must not oversell; an operator reading the log
        // should learn the limits without opening the source.
        assert!(Policy::Bitcoin(bitcoin::Network::Bitcoin)
            .describe()
            .contains("retarget schedule NOT validated"));
        assert!(Policy::AuxPow.describe().contains("proof of work NOT validated"));
        assert!(Policy::Continuity.describe().contains("no proof of work"));
    }
}
