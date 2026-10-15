//! Commit-stall fix (2026-10-14): near-tip decision and the stall guard.
//! The engine-level reproduction lives in
//! crates/metashrew-sync/tests/commit_stall_reorg_test.rs.

use crate::{near_tip_action, NearTipAction, StallGuard, LAST_SENT_UNSET};

#[test]
fn rollback_resets_watermark_and_sends_the_rollback_height() {
    // Fetcher had sent 971345; the reorg check rolled storage back to
    // 971344 and the engine handed back 971345 again. Pre-fix this was
    // dropped as a duplicate (and with the floor it was never even
    // returned), so 971345 was never re-applied.
    assert_eq!(
        near_tip_action(971_345, 971_345, true),
        NearTipAction::Send { reset_watermark: true }
    );
    assert_eq!(
        near_tip_action(971_350, 971_345, true),
        NearTipAction::Send { reset_watermark: true }
    );
}

#[test]
fn without_rollback_the_rc7_dedup_still_applies() {
    assert_eq!(near_tip_action(100, 100, false), NearTipAction::Drop);
    assert_eq!(near_tip_action(100, 99, false), NearTipAction::Drop);
    assert_eq!(
        near_tip_action(100, 101, false),
        NearTipAction::Send { reset_watermark: false }
    );
    assert_eq!(
        near_tip_action(LAST_SENT_UNSET, 5, false),
        NearTipAction::Send { reset_watermark: false }
    );
}

#[test]
fn stall_guard_exits_only_after_consecutive_failures() {
    let mut g = StallGuard::new(3);
    assert!(!g.record_failure());
    assert!(!g.record_failure());
    g.record_success();
    assert!(!g.record_failure());
    assert!(!g.record_failure());
    assert!(g.record_failure(), "third consecutive failure must trip the guard");
}

#[test]
fn stall_guard_zero_disables() {
    let mut g = StallGuard::new(0);
    for _ in 0..1000 {
        assert!(!g.record_failure());
    }
}
