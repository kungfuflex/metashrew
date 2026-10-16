//! v9.0.5-rc.6 fetcher-dedup tests.
//!
//! These tests pin down the pure-function `compute_next_fetch_range`
//! helper used by the prefetcher loop in `crate::run`.  The function
//! is the structural fix for the production incident on the mainnet
//! `metashrew-v9-0-4-mainnet-pool-v904rc2-0` pod (2026-05-17
//! 00:21–00:38):
//!
//! - The fetcher had been computing `fetch_start =
//!   engine.current_height()` every loop iteration.
//! - The engine's `current_height` is the PROCESSOR's committed tip
//!   — it lags behind what the fetcher has already enqueued onto the
//!   prefetch channel.
//! - When the processor was slow, the fetcher would loop, re-read
//!   the unchanged tip, and re-enqueue the same range, which the
//!   processor then rejected with "out-of-order commit rejected"
//!   inside `commit_atomic`, which triggered the rc.3 infinite-retry
//!   backoff and stuck the pod.
//!
//! The fix: the fetcher tracks its OWN watermark (`last_sent`)
//! separately from the processor's tip.  This file pins down the
//! arithmetic of that watermark.
//!
//! Invariants pinned here:
//!   1. Slow processor: the channel must never accumulate duplicates
//!      of height N when the processor tip stays at N across many
//!      iterations.
//!   2. Rollback: when the processor tip drops by K, the fetcher's
//!      `last_sent_height` resets to UNSET and the next iteration
//!      starts re-fetching from the new tip.
//!   3. Linear advance: when the processor advances atomically, the
//!      fetcher sends each height EXACTLY once.

use crate::{
    compute_next_fetch_range, near_tip_min_send_height, near_tip_should_drop, LAST_SENT_UNSET,
};

// ----------------------------------------------------------------------------
// Test 1: slow processor → no duplicates
//
// Simulates 30 loop iterations where the processor tip is stuck at N
// because the processor is slow.  After the first iteration, the
// fetcher's `last_sent` is N + prefetch_size - 1.  Subsequent
// iterations must NOT re-fetch [N, N+prefetch_size); they must
// return None (nothing new to fetch) since `last_sent` is already at
// the prefetch boundary.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_does_not_duplicate_on_slow_processor() {
    let processor_tip = 949_719u32;
    let remote_tip = 950_000u32;
    let prefetch_size = 6usize;
    let mut last_sent = LAST_SENT_UNSET;

    // First iteration: starts from processor_tip.
    let (start1, end1) =
        compute_next_fetch_range(processor_tip, last_sent, prefetch_size, remote_tip)
            .expect("first iteration must produce a range");
    assert_eq!(start1, 949_719);
    assert_eq!(end1, 949_725, "fetch_end = fetch_start + prefetch_size");

    // Simulate the fetcher sending all of [start1, end1) successfully:
    // last_sent = end1 - 1 = 949_724.
    last_sent = (end1 - 1) as i64;

    // Now collect ALL ranges produced over the next 30 iterations
    // while the processor remains stuck at 949_719.  The buffer
    // would block in production; for the pure function we just
    // verify NO range overlaps with [949_719, 949_725).
    let mut all_heights = Vec::new();
    all_heights.extend(start1..end1);
    for _ in 0..30 {
        if let Some((s, e)) =
            compute_next_fetch_range(processor_tip, last_sent, prefetch_size, remote_tip)
        {
            for h in s..e {
                assert!(
                    !all_heights.contains(&h),
                    "fetcher tried to re-enqueue height {} (already in {:?})",
                    h,
                    all_heights,
                );
                all_heights.push(h);
            }
            last_sent = (e - 1) as i64;
        }
    }

    // All heights MUST be unique.
    let mut sorted = all_heights.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        all_heights.len(),
        "all enqueued heights must be unique; got duplicates: {:?}",
        all_heights,
    );

    // And the first 6 heights are exactly [949_719..949_725).
    assert_eq!(
        all_heights[..6],
        [949_719, 949_720, 949_721, 949_722, 949_723, 949_724][..],
        "first batch must be the initial prefetch range",
    );
}

// ----------------------------------------------------------------------------
// Test 2: rollback resets the fetcher.
//
// Simulates: fetcher had enqueued through 949_724.  Processor hits a
// reorg and the rollback handler drops the processor tip to 949_715.
// In the live code, the rollback handler also resets
// `last_sent_height` to UNSET. Verify that with UNSET, the next call
// fetches from the rolled-back processor tip.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_resumes_after_rollback() {
    let prefetch_size = 6usize;
    let remote_tip = 950_000u32;

    // Pre-rollback state: processor advanced and last_sent climbed.
    let mut last_sent = 949_724i64;
    let processor_tip_before = 949_720u32;

    // Sanity: in this pre-rollback state, fetcher would skip past
    // last_sent.  It would NOT re-fetch 949_720.
    let (s_before, _e_before) =
        compute_next_fetch_range(processor_tip_before, last_sent, prefetch_size, remote_tip)
            .expect("pre-rollback iteration must produce a range");
    assert_eq!(
        s_before, 949_725,
        "without rollback, fetcher continues from last_sent + 1"
    );

    // Now a reorg fires.  The processor tip drops to 949_715, and the
    // rollback handler resets last_sent to UNSET (this is the
    // contract: the fetcher's watermark MUST be cleared on rollback).
    last_sent = LAST_SENT_UNSET;
    let processor_tip_after = 949_715u32;

    let (s_after, e_after) =
        compute_next_fetch_range(processor_tip_after, last_sent, prefetch_size, remote_tip)
            .expect("post-rollback iteration must produce a range");
    assert_eq!(
        s_after, 949_715,
        "post-rollback fetch must start at the new processor tip"
    );
    assert_eq!(
        e_after, 949_721,
        "post-rollback batch must size up to prefetch_size"
    );
}

// ----------------------------------------------------------------------------
// Test 3: linear processor advance → each height sent exactly once.
//
// Drive the loop forward: processor advances one block at a time, in
// lockstep with the fetcher (the slowest realistic case where
// last_sent stays at most prefetch_size ahead of the processor tip).
// Each height in [N, N+50] must appear EXACTLY once across all
// fetches.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_advances_with_processor() {
    let prefetch_size = 6usize;
    let remote_tip = 950_000u32;
    let start_tip = 949_719u32;
    let mut processor_tip = start_tip;
    let mut last_sent = LAST_SENT_UNSET;
    let mut all_heights = Vec::new();

    // Run 200 iterations: alternate "fetcher fetches a range" with
    // "processor consumes one block".  Stop when we've covered at
    // least 50 heights.
    let mut iters = 0;
    while all_heights.len() < 50 && iters < 200 {
        iters += 1;
        if let Some((s, e)) =
            compute_next_fetch_range(processor_tip, last_sent, prefetch_size, remote_tip)
        {
            for h in s..e {
                all_heights.push(h);
            }
            last_sent = (e - 1) as i64;
        }
        // Simulate processor consuming one block per outer iteration.
        if processor_tip < remote_tip {
            processor_tip += 1;
        }
    }

    // Uniqueness invariant.
    let mut sorted = all_heights.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        all_heights.len(),
        "linear advance must send each height exactly once; got duplicates: {:?}",
        all_heights,
    );

    // Contiguity invariant: the heights we sent should be a
    // contiguous range starting at start_tip.
    assert_eq!(
        all_heights[0], start_tip,
        "first sent must be the initial processor tip"
    );
    for (i, h) in all_heights.iter().enumerate() {
        assert_eq!(
            *h,
            start_tip + i as u32,
            "fetcher must send heights contiguously from start_tip",
        );
    }
}

// ----------------------------------------------------------------------------
// Test 4: remote_tip cap honored.
//
// Fetcher must not try to fetch beyond remote_tip.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_caps_at_remote_tip() {
    let prefetch_size = 6usize;
    let processor_tip = 949_999u32;
    let remote_tip = 950_001u32;

    let (s, e) =
        compute_next_fetch_range(processor_tip, LAST_SENT_UNSET, prefetch_size, remote_tip)
            .expect("range must exist when processor < remote");
    assert_eq!(s, 949_999);
    assert_eq!(
        e, 950_002,
        "fetch_end caps at remote_tip + 1, NOT at prefetch_size",
    );
}

// ----------------------------------------------------------------------------
// Test 5: nothing to do.
//
// When processor_tip == remote_tip and last_sent == remote_tip,
// the fetcher has nothing to do.  Must return None.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_returns_none_when_caught_up() {
    let remote_tip = 950_000u32;
    let processor_tip = 950_000u32;
    let last_sent = 950_000i64;

    let res = compute_next_fetch_range(processor_tip, last_sent, 6, remote_tip);
    assert!(
        res.is_none(),
        "fetcher must return None when fully caught up; got {:?}",
        res,
    );
}

// ----------------------------------------------------------------------------
// Test 6: processor advances past last_sent (clean reorg-rollback up
// case — shouldn't happen since rollback only goes DOWN, but pin the
// invariant anyway).
//
// If for any reason processor_tip > last_sent + 1, the fetcher
// should skip the gap and start at processor_tip (we DO NOT want
// to re-fetch heights the processor already has).
// ----------------------------------------------------------------------------
#[test]
fn fetcher_jumps_to_processor_tip_if_ahead_of_last_sent() {
    let prefetch_size = 6usize;
    let processor_tip = 949_730u32;
    let last_sent = 949_720i64;
    let remote_tip = 950_000u32;

    let (s, e) = compute_next_fetch_range(processor_tip, last_sent, prefetch_size, remote_tip)
        .expect("range must exist");
    assert_eq!(
        s, 949_730,
        "must skip ahead to processor_tip, not stick at last_sent + 1",
    );
    assert_eq!(e, 949_736);
}

// ============================================================================
// v9.0.5-rc.7 NEAR-TIP path dedup tests.
//
// rc.6 fixed the batch-prefetch path but the near-tip single-block
// path was still vulnerable: when `remote_tip - fetch_start <=
// reorg_check_threshold` (default 6), the fetcher called
// `engine.get_next_block_data()` and sent whatever height the engine
// returned, with NO check against `last_sent_height`.  When the
// processor was slow on block N, `engine.current_height()` stayed at
// N while the fetcher's `last_sent_height` had already climbed (in a
// previous near-tip iteration that successfully bumped it).  On the
// next loop, `get_next_block_data` returned N again, the send went
// through, and the processor saw a duplicate N.
//
// Production evidence on v9.0.5-rc.6 mainnet pod (block 949728):
//   01:15:17 INFO Fetched block 949728 (1750254 bytes)
//   01:15:17 INFO Fetched block 949728 (1750254 bytes)  <- DUPLICATE
//   01:16:47 processed block 949728 atomically
//   ... 949729..949733 process ...
//   01:20:58 processed block 949728 atomically   <- second copy
//   01:20:58 WARN out-of-order commit rejected: attempted 949728,
//                 current tip 949733
//
// The tests below pin down the dedup invariant via the pure helpers
// `near_tip_min_send_height` and `near_tip_should_drop`.
// ============================================================================

// ----------------------------------------------------------------------------
// Test 7: stuck atomic in near-tip path → DROP every duplicate of
// last_sent.
//
// Simulates `engine.get_next_block_data()` returning the same height
// for many consecutive calls (because `current_height` hasn't
// advanced yet).  The decision helper MUST drop every one of them.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_near_tip_does_not_dup() {
    // The fetcher already sent 949_728 successfully.
    let last_sent = 949_728i64;

    // The processor atomic is stuck at 949_728 (the block is still
    // being processed).  `get_next_block_data` returns 949_728
    // repeatedly.  Every iteration must DROP.
    let mut sent_anything = false;
    for _ in 0..20 {
        let candidate = 949_728u32;
        if near_tip_should_drop(last_sent, candidate) {
            // Drop: do not send.
            continue;
        }
        sent_anything = true;
    }
    assert!(
        !sent_anything,
        "near-tip path must drop every duplicate of last_sent; sent at least one",
    );

    // Defensive: even a candidate BELOW last_sent (handle_reorg
    // lowered current_height) must be dropped.
    assert!(
        near_tip_should_drop(last_sent, 949_727),
        "candidate < last_sent must be dropped (defensive against handle_reorg lowering current_height)",
    );
    assert!(
        near_tip_should_drop(last_sent, 949_000),
        "candidate way below last_sent must be dropped",
    );
}

// ----------------------------------------------------------------------------
// Test 8: near-tip path resumes once the atomic advances.
//
// Same setup as Test 7, but after several iterations the processor
// commits 949_728 and `get_next_block_data` starts returning 949_729.
// The dedup helper MUST allow 949_729 through (and any height >=
// last_sent + 1).
// ----------------------------------------------------------------------------
#[test]
fn fetcher_near_tip_resumes_when_atomic_advances() {
    let last_sent = 949_728i64;

    // While the atomic is stuck: 949_728 is dropped.
    assert!(near_tip_should_drop(last_sent, 949_728));

    // Atomic advances by one: 949_729 must go through.
    assert!(
        !near_tip_should_drop(last_sent, 949_729),
        "next height (last_sent + 1) must NOT be dropped — that's the resume point",
    );

    // Heights further ahead must also go through.
    assert!(!near_tip_should_drop(last_sent, 949_730));
    assert!(!near_tip_should_drop(last_sent, 950_000));
}

// ----------------------------------------------------------------------------
// Test 9: cold start (LAST_SENT_UNSET) accepts anything.
//
// On the first iteration the fetcher has nothing in flight; the
// near-tip path must allow whatever `get_next_block_data` returns
// (including 0 if we're indexing genesis).
// ----------------------------------------------------------------------------
#[test]
fn fetcher_near_tip_cold_start_accepts_anything() {
    let last_sent = LAST_SENT_UNSET;
    assert_eq!(near_tip_min_send_height(last_sent), 0);
    assert!(!near_tip_should_drop(last_sent, 0));
    assert!(!near_tip_should_drop(last_sent, 949_728));
    assert!(!near_tip_should_drop(last_sent, u32::MAX));
}

// ----------------------------------------------------------------------------
// Test 10: reproduction of the production incident.
//
// Drives the near-tip dedup decision through the exact sequence
// observed on the v9.0.5-rc.6 mainnet pod for block 949_728.  In rc.6
// the fetcher sent 949_728 twice.  With the rc.7 guard, the second
// duplicate is dropped.
// ----------------------------------------------------------------------------
#[test]
fn fetcher_near_tip_repro_949728_dup() {
    // ----- Iteration 1: cold-ish — last_sent = 949_727 from a
    // previous near-tip iteration.  `get_next_block_data` returns
    // 949_728 (processor caught up).  Must accept.
    let mut last_sent = 949_727i64;
    let candidate = 949_728u32;
    assert!(
        !near_tip_should_drop(last_sent, candidate),
        "first sighting of 949_728 must be accepted",
    );
    // Simulate successful send: bump watermark.
    last_sent = candidate as i64;

    // ----- Iteration 2: processor is still grinding on 949_728
    // (took 90 seconds in production).  `get_next_block_data`
    // re-reads the same atomic and returns 949_728 again.  In rc.6
    // this was sent through and caused the bug.  Must DROP now.
    let candidate_again = 949_728u32;
    assert!(
        near_tip_should_drop(last_sent, candidate_again),
        "second sighting of 949_728 in same near-tip pass MUST be dropped (rc.6 production bug)",
    );

    // Watermark must not advance on the dropped iteration.
    assert_eq!(last_sent, 949_728);

    // ----- Iteration 3: processor finally commits 949_728.
    // `get_next_block_data` returns 949_729.  Must accept.
    let candidate_next = 949_729u32;
    assert!(
        !near_tip_should_drop(last_sent, candidate_next),
        "next height after the dup-storm must be accepted",
    );
}
