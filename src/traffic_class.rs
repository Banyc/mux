use crate::dual_lane::LaneClass;
use std::time::Duration;
use tokio::time::Instant;
pub const BULK_THRESHOLD: usize = 2 * 1024;
pub(crate) const PROMOTE_IMMEDIATE_THRESHOLD: usize = 32 * 1024;
pub(crate) const DEMOTE_STREAK: usize = 4;
pub(crate) const MIGRATION_COOLDOWN: Duration = Duration::from_millis(150);
pub(crate) const HISTORY_MAX: usize = 16;
pub(crate) const HISTORY_MIN: usize = 3;
pub(crate) fn is_bulk_size(size: usize) -> bool {
    size > BULK_THRESHOLD
}
impl LaneMigrationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LargeWrite => "large_write",
            Self::BulkRatio => "bulk_ratio",
            Self::SmallWriteStreak => "small_write_streak",
            Self::Forced => "forced",
            Self::Rebind => "rebind",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneMigrationReason {
    LargeWrite,
    BulkRatio,
    SmallWriteStreak,
    Forced,
    Rebind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneMigration {
    pub target: LaneClass,
    pub reason: LaneMigrationReason,
    pub write_size: usize,
    pub small_writes: u32,
    pub bulk_writes: u32,
    pub small_streak: usize,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct Classifier {
    small_count: u32,
    bulk_count: u32,
}
impl Classifier {
    pub(crate) fn new() -> Self {
        Self::default()
    }
    pub(crate) fn record(&mut self, size: usize) {
        if self.small_count + self.bulk_count >= HISTORY_MAX as u32 {
            self.small_count /= 2;
            self.bulk_count /= 2;
        }
        if is_bulk_size(size) {
            self.bulk_count += 1;
        } else {
            self.small_count += 1;
        }
    }
    pub(crate) fn is_bulk(&self) -> bool {
        let total = self.small_count + self.bulk_count;
        if total < HISTORY_MIN as u32 {
            return false;
        }
        self.small_count * 3 < total * 2
    }
}
#[derive(Debug)]
pub(crate) struct LanePolicy {
    classifier: Classifier,
    small_streak: usize,
    last_migration: Option<Instant>,
}
impl LanePolicy {
    pub(crate) fn new() -> Self {
        Self {
            classifier: Classifier::new(),
            small_streak: 0,
            last_migration: None,
        }
    }
    pub(crate) fn on_write(
        &mut self,
        size: usize,
        current: Option<LaneClass>,
        now: Instant,
    ) -> Option<LaneMigration> {
        self.classifier.record(size);
        let current = current?;
        let (target, reason) = match current {
            LaneClass::Interactive => {
                let reason = if size >= PROMOTE_IMMEDIATE_THRESHOLD {
                    LaneMigrationReason::LargeWrite
                } else if self.classifier.is_bulk() {
                    LaneMigrationReason::BulkRatio
                } else {
                    return None;
                };
                (LaneClass::Bulk, reason)
            }
            LaneClass::Bulk => {
                if is_bulk_size(size) {
                    self.small_streak = 0;
                } else {
                    self.small_streak += 1;
                }
                if self.small_streak < DEMOTE_STREAK || self.classifier.is_bulk() {
                    return None;
                }
                (
                    LaneClass::Interactive,
                    LaneMigrationReason::SmallWriteStreak,
                )
            }
        };
        if !self.cooled(now) {
            return None;
        }
        Some(self.decision(target, reason, size))
    }
    pub(crate) fn decision(
        &self,
        target: LaneClass,
        reason: LaneMigrationReason,
        write_size: usize,
    ) -> LaneMigration {
        LaneMigration {
            target,
            reason,
            write_size,
            small_writes: self.classifier.small_count,
            bulk_writes: self.classifier.bulk_count,
            small_streak: self.small_streak,
        }
    }
    pub(crate) fn note_migration(&mut self, now: Instant) {
        self.last_migration = Some(now);
        self.small_streak = 0;
    }
    fn cooled(&self, now: Instant) -> bool {
        match self.last_migration {
            None => true,
            Some(last) => now.duration_since(last) >= MIGRATION_COOLDOWN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn on_write(
        policy: &mut LanePolicy,
        size: usize,
        current: Option<LaneClass>,
        now: Instant,
    ) -> Option<LaneClass> {
        policy.on_write(size, current, now).map(|d| d.target)
    }

    #[test]
    fn classifier_tracks_bulk_ratio() {
        let mut c = Classifier::new();
        for _ in 0..10 {
            c.record(100);
        }
        assert!(!c.is_bulk(), "all small writes = not bulk");
        for _ in 0..10 {
            c.record(3000);
        }
        assert!(c.is_bulk(), "many large writes = bulk");
        for _ in 0..20 {
            c.record(100);
        }
        assert!(!c.is_bulk(), "halving should let small wins dominate");
    }

    #[test]
    fn classifier_needs_min_observations() {
        let mut c = Classifier::new();
        c.record(3000);
        c.record(3000);
        assert!(!c.is_bulk(), "below HISTORY_MIN = latency-sensitive");
        c.record(3000);
        assert!(c.is_bulk(), "HISTORY_MIN all-bulk observations = bulk");
    }

    #[test]
    fn threshold_boundary_is_exclusive() {
        assert!(!is_bulk_size(BULK_THRESHOLD));
        assert!(is_bulk_size(BULK_THRESHOLD + 1));
    }

    const INT: Option<LaneClass> = Some(LaneClass::Interactive);
    const BULK: Option<LaneClass> = Some(LaneClass::Bulk);

    #[test]
    fn single_moderate_write_does_not_promote() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(on_write(&mut p, 3000, INT, now), None);
    }

    #[test]
    fn ratio_promotes_after_min_observations() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(on_write(&mut p, 3000, INT, now), None);
        assert_eq!(on_write(&mut p, 3000, INT, now), None);
        assert_eq!(on_write(&mut p, 3000, INT, now), Some(LaneClass::Bulk));
    }

    #[test]
    fn huge_write_promotes_immediately() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(
            on_write(&mut p, PROMOTE_IMMEDIATE_THRESHOLD, INT, now),
            Some(LaneClass::Bulk)
        );
    }

    #[test]
    fn promote_respects_cooldown() {
        let mut p = LanePolicy::new();
        let t0 = Instant::now();
        p.note_migration(t0);
        for _ in 0..HISTORY_MIN {
            assert_eq!(
                on_write(&mut p, 3000, INT, t0 + Duration::from_millis(10)),
                None
            );
        }
        assert_eq!(
            on_write(
                &mut p,
                PROMOTE_IMMEDIATE_THRESHOLD,
                INT,
                t0 + Duration::from_millis(10)
            ),
            None
        );
        let later = t0 + MIGRATION_COOLDOWN + Duration::from_millis(1);
        assert_eq!(on_write(&mut p, 3000, INT, later), Some(LaneClass::Bulk));
    }

    #[test]
    fn no_promote_on_small_writes() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..10 {
            assert_eq!(on_write(&mut p, 100, INT, now), None);
        }
    }

    #[test]
    fn coalescing_artifacts_do_not_flap() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..64 {
            assert_eq!(on_write(&mut p, 300, INT, now), None);
            assert_eq!(on_write(&mut p, 300, INT, now), None);
            assert_eq!(on_write(&mut p, 4096, INT, now), None);
            assert_eq!(on_write(&mut p, 300, INT, now), None);
        }
    }

    #[test]
    fn demote_after_streak_first_migration_immediate() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(on_write(&mut p, 100, BULK, now), None);
        }
        assert_eq!(
            on_write(&mut p, 100, BULK, now),
            Some(LaneClass::Interactive)
        );
    }

    #[test]
    fn demote_respects_cooldown_then_fires() {
        let mut p = LanePolicy::new();
        let t0 = Instant::now();
        p.note_migration(t0);
        for _ in 0..DEMOTE_STREAK + 2 {
            assert_eq!(
                on_write(&mut p, 100, BULK, t0 + Duration::from_millis(10)),
                None
            );
        }
        let later = t0 + MIGRATION_COOLDOWN + Duration::from_millis(1);
        assert_eq!(
            on_write(&mut p, 100, BULK, later),
            Some(LaneClass::Interactive)
        );
    }

    #[test]
    fn demote_blocked_while_history_reads_bulk() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..HISTORY_MAX {
            assert_eq!(on_write(&mut p, 3000, BULK, now), None);
        }
        for _ in 0..DEMOTE_STREAK {
            assert_eq!(on_write(&mut p, 100, BULK, now), None);
        }
        let mut demoted = false;
        for _ in 0..2 * HISTORY_MAX {
            if on_write(&mut p, 100, BULK, now) == Some(LaneClass::Interactive) {
                demoted = true;
                break;
            }
        }
        assert!(demoted, "sustained small writes must eventually demote");
    }

    #[test]
    fn migrating_state_records_but_never_decides() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(on_write(&mut p, 50_000, None, now), None);
    }

    #[test]
    fn large_write_resets_demote_streak() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(on_write(&mut p, 100, BULK, now), None);
        }
        assert_eq!(on_write(&mut p, 3000, BULK, now), None);
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(on_write(&mut p, 100, BULK, now), None);
        }
    }

    #[test]
    fn migration_decisions_carry_their_reason() {
        let now = Instant::now();
        let mut p = LanePolicy::new();
        let huge = p
            .on_write(PROMOTE_IMMEDIATE_THRESHOLD, INT, now)
            .expect("a huge write promotes at once");
        assert_eq!(huge.reason, LaneMigrationReason::LargeWrite);
        assert_eq!(huge.write_size, PROMOTE_IMMEDIATE_THRESHOLD);
        assert_eq!(huge.bulk_writes, 1);
        let mut p = LanePolicy::new();
        for _ in 0..HISTORY_MIN - 1 {
            assert!(p.on_write(3000, INT, now).is_none());
        }
        let ratio = p.on_write(3000, INT, now).expect("the mix reads as bulk");
        assert_eq!(ratio.reason, LaneMigrationReason::BulkRatio);
        assert_eq!(ratio.bulk_writes, HISTORY_MIN as u32);
        assert_eq!(ratio.small_writes, 0);
        let mut p = LanePolicy::new();
        for _ in 0..DEMOTE_STREAK - 1 {
            assert!(p.on_write(100, BULK, now).is_none());
        }
        let demote = p.on_write(100, BULK, now).expect("a small-write streak");
        assert_eq!(demote.reason, LaneMigrationReason::SmallWriteStreak);
        assert_eq!(demote.small_streak, DEMOTE_STREAK);
        let forced = p.decision(LaneClass::Bulk, LaneMigrationReason::Forced, 0);
        assert_eq!(forced.reason.as_str(), "forced");
        assert_eq!(forced.small_writes, demote.small_writes);
    }
}
