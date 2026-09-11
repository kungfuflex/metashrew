//! Guest-side helpers for writing a custom validator in Rust.
//!
//! A custom validator is a wasm module with one export and two imports:
//!
//! ```text
//! import  env.__request_len() -> i32     how many bytes is my request
//! import  env.__load_request(ptr: i32)   copy it to ptr
//! export  validate() -> i32              pointer to [len:u32 LE | verdict]
//! ```
//!
//! Nothing else is available. The host defines every other import as a
//! trap, so a module that reaches for storage or the network fails to
//! run rather than silently getting nothing.
//!
//! The whole of a validator using these helpers:
//!
//! ```rust,ignore
//! use metashrew_validator_core::{guest, ValidationRequest, Verdict, block_hash};
//!
//! #[no_mangle]
//! pub extern "C" fn validate() -> i32 {
//!     guest::run(|req: ValidationRequest| {
//!         if block_hash(&req.header[..80]) != req.claimed_hash {
//!             return Verdict::reject("header does not hash to the claimed id");
//!         }
//!         Verdict::accept()
//!     })
//! }
//! ```
//!
//! Build it with `cargo build --release --target wasm32-unknown-unknown`
//! and pass it as `--spv-type custom:path/to/validator.wasm`.

use crate::wire::{ValidationRequest, Verdict};

#[cfg(target_arch = "wasm32")]
extern "C" {
    fn __request_len() -> i32;
    fn __load_request(ptr: i32);
}

/// Read the request the host staged for this call.
#[cfg(target_arch = "wasm32")]
pub fn request() -> Option<ValidationRequest> {
    let len = unsafe { __request_len() } as usize;
    let mut buf = vec![0u8; len];
    unsafe { __load_request(buf.as_mut_ptr() as i32) };
    ValidationRequest::decode(&buf)
}

/// Native builds have no host to ask, so this is `None`. Present so a
/// validator crate compiles (and unit-tests its policy) off-target.
#[cfg(not(target_arch = "wasm32"))]
pub fn request() -> Option<ValidationRequest> {
    None
}

/// Hand a verdict back to the host.
///
/// Returns a pointer to `[len:u32 LE | payload]`, leaking the buffer:
/// the module is instantiated fresh per block and torn down immediately
/// after, so there is nothing to leak into.
pub fn respond(verdict: &Verdict) -> i32 {
    let payload = verdict.encode();
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&payload);

    let boxed = framed.into_boxed_slice();
    let ptr = Box::into_raw(boxed) as *mut u8;
    // Point at the payload; the host reads the length from ptr-4.
    (ptr as usize as i32) + 4
}

/// Decode the request, apply `policy`, and encode the verdict.
///
/// A request that will not decode is rejected rather than accepted —
/// a validator that cannot see what it is judging must not approve it.
pub fn run<F>(policy: F) -> i32
where
    F: FnOnce(ValidationRequest) -> Verdict,
{
    match request() {
        Some(req) => respond(&policy(req)),
        None => respond(&Verdict::reject(
            "validator could not decode its request; refusing to accept the block",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn respond_frames_the_payload_with_a_length_prefix() {
        let v = Verdict::reject("nope");
        let ptr = respond(&v);

        // Reconstruct what the host would read.
        let base = (ptr - 4) as usize as *const u8;
        let (len, payload) = unsafe {
            let len = u32::from_le_bytes(std::slice::from_raw_parts(base, 4).try_into().unwrap());
            let payload = std::slice::from_raw_parts(base.add(4), len as usize).to_vec();
            (len, payload)
        };
        assert_eq!(len as usize, v.encode().len());
        assert_eq!(Verdict::decode(&payload), Some(v));
    }

    #[test]
    fn an_undecodable_request_rejects_rather_than_accepts() {
        // Off-target `request()` is always None, which exercises exactly
        // the fail-closed path.
        let ptr = run(|_| Verdict::accept());
        let base = (ptr - 4) as usize as *const u8;
        let payload = unsafe {
            let len = u32::from_le_bytes(std::slice::from_raw_parts(base, 4).try_into().unwrap());
            std::slice::from_raw_parts(base.add(4), len as usize).to_vec()
        };
        let verdict = Verdict::decode(&payload).unwrap();
        assert!(!verdict.accepted);
        assert!(verdict.reason.contains("refusing to accept"));
    }
}
