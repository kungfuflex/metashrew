//! The request/verdict encoding crossing the wasm boundary.
//!
//! Native validators take [`ValidationRequest`] directly; a custom wasm
//! validator receives the same value encoded by [`ValidationRequest::encode`]
//! and returns a [`Verdict`] encoded by [`Verdict::encode`]. Keeping one
//! type for both means a policy can be prototyped natively and shipped
//! as wasm without its logic changing.
//!
//! Both encodings are length-prefixed and version-tagged. They are
//! deliberately dull: this data crosses a sandbox boundary from an
//! untrusted module, so every decode is bounds-checked and returns an
//! `Option` rather than panicking.

/// Wire version. Bump on any layout change; decoders reject anything else.
pub const WIRE_VERSION: u8 = 1;

/// One block, presented for validation against its parent.
///
/// This is the validator's entire world: no database handle, no network,
/// no history beyond the parent hash. A validator is a pure function of
/// this struct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationRequest {
    /// Height being validated.
    pub height: u32,
    /// Raw block header. At least 80 bytes; merged-mining chains carry
    /// more, and a policy that understands them may read past 80.
    pub header: Vec<u8>,
    /// The hash the node claims for this height, display order.
    pub claimed_hash: [u8; 32],
    /// Our stored hash for `height - 1`, display order. `None` at
    /// genesis, or when the parent's hash is missing from storage —
    /// which every built-in policy treats as a rejection above height 0.
    pub prev_hash: Option<[u8; 32]>,
}

impl ValidationRequest {
    /// `version:u8 | height:u32 LE | flags:u8 | claimed_hash:32 |
    ///  header_len:u32 LE | header | prev_hash:32?`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 4 + 1 + 32 + 4 + self.header.len() + 32);
        out.push(WIRE_VERSION);
        out.extend_from_slice(&self.height.to_le_bytes());
        out.push(if self.prev_hash.is_some() { 1 } else { 0 });
        out.extend_from_slice(&self.claimed_hash);
        out.extend_from_slice(&(self.header.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.header);
        if let Some(p) = &self.prev_hash {
            out.extend_from_slice(p);
        }
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        let mut at = 0usize;
        let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
            let s = b.get(*at..*at + n)?;
            *at += n;
            Some(s)
        };

        if *take(&mut at, 1)?.first()? != WIRE_VERSION {
            return None;
        }
        let height = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?);
        let has_prev = take(&mut at, 1)?[0] == 1;
        let claimed_hash: [u8; 32] = take(&mut at, 32)?.try_into().ok()?;
        let header_len = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
        // Bound the length against what is actually present before
        // allocating: this buffer may come from an untrusted module.
        if header_len > b.len() {
            return None;
        }
        let header = take(&mut at, header_len)?.to_vec();
        let prev_hash = if has_prev {
            Some(take(&mut at, 32)?.try_into().ok()?)
        } else {
            None
        };

        Some(ValidationRequest { height, header, claimed_hash, prev_hash })
    }
}

/// A validator's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Verdict {
    pub accepted: bool,
    /// Why it was rejected. Empty when accepted. Surfaced verbatim in
    /// the sync error, so it should read as an explanation on its own.
    pub reason: String,
}

impl Verdict {
    pub fn accept() -> Self {
        Verdict { accepted: true, reason: String::new() }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Verdict { accepted: false, reason: reason.into() }
    }

    /// `version:u8 | accepted:u8 | reason_len:u32 LE | reason`
    pub fn encode(&self) -> Vec<u8> {
        let r = self.reason.as_bytes();
        let mut out = Vec::with_capacity(1 + 1 + 4 + r.len());
        out.push(WIRE_VERSION);
        out.push(if self.accepted { 1 } else { 0 });
        out.extend_from_slice(&(r.len() as u32).to_le_bytes());
        out.extend_from_slice(r);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 6 || b[0] != WIRE_VERSION {
            return None;
        }
        let reason_len = u32::from_le_bytes(b[2..6].try_into().ok()?) as usize;
        let reason = b.get(6..6 + reason_len)?;
        Some(Verdict {
            accepted: b[1] == 1,
            reason: String::from_utf8_lossy(reason).into_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ValidationRequest {
        ValidationRequest {
            height: 812_345,
            header: vec![0xab; 80],
            claimed_hash: [0x11; 32],
            prev_hash: Some([0x22; 32]),
        }
    }

    #[test]
    fn request_round_trips_with_and_without_a_parent() {
        let r = sample();
        assert_eq!(ValidationRequest::decode(&r.encode()), Some(r.clone()));

        let genesis = ValidationRequest { height: 0, prev_hash: None, ..r };
        assert_eq!(ValidationRequest::decode(&genesis.encode()), Some(genesis));
    }

    #[test]
    fn request_round_trips_an_oversized_auxpow_header() {
        let r = ValidationRequest { header: vec![0x5a; 1024], ..sample() };
        assert_eq!(ValidationRequest::decode(&r.encode()), Some(r));
    }

    #[test]
    fn verdict_round_trips() {
        assert_eq!(Verdict::decode(&Verdict::accept().encode()), Some(Verdict::accept()));
        let r = Verdict::reject("block 7 does not meet its own stated difficulty");
        assert_eq!(Verdict::decode(&r.encode()), Some(r));
    }

    /// These buffers come out of a sandbox, so malformed input must
    /// decode to None rather than panic or over-allocate.
    #[test]
    fn truncated_or_hostile_buffers_decode_to_none() {
        let good = sample().encode();
        for cut in 0..good.len() {
            assert!(
                ValidationRequest::decode(&good[..cut]).is_none()
                    || cut == good.len(),
                "prefix of length {cut} decoded"
            );
        }
        // A header_len claiming 4 GiB must not allocate.
        let mut hostile = sample().encode();
        hostile[38..42].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(ValidationRequest::decode(&hostile), None);

        assert_eq!(Verdict::decode(&[]), None);
        assert_eq!(Verdict::decode(&[WIRE_VERSION, 1, 0xff, 0xff, 0xff, 0xff]), None);
    }

    #[test]
    fn a_foreign_wire_version_is_rejected() {
        let mut b = sample().encode();
        b[0] = WIRE_VERSION + 1;
        assert_eq!(ValidationRequest::decode(&b), None);

        let mut v = Verdict::accept().encode();
        v[0] = WIRE_VERSION + 1;
        assert_eq!(Verdict::decode(&v), None);
    }
}
