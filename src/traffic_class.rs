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
    ) -> Option<LaneClass> {
        self.classifier.record(size);
        let current = current?;
        match current {
            LaneClass::Interactive => {
                let promote = size >= PROMOTE_IMMEDIATE_THRESHOLD || self.classifier.is_bulk();
                if promote && self.cooled(now) {
                    return Some(LaneClass::Bulk);
                }
            }
            LaneClass::Bulk => {
                if is_bulk_size(size) {
                    self.small_streak = 0;
                } else {
                    self.small_streak += 1;
                }
                if self.small_streak >= DEMOTE_STREAK
                    && !self.classifier.is_bulk()
                    && self.cooled(now)
                {
                    return Some(LaneClass::Interactive);
                }
            }
        }
        None
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
        assert_eq!(p.on_write(3000, INT, now), None);
    }

    #[test]
    fn ratio_promotes_after_min_observations() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(p.on_write(3000, INT, now), None);
        assert_eq!(p.on_write(3000, INT, now), None);
        assert_eq!(p.on_write(3000, INT, now), Some(LaneClass::Bulk));
    }

    #[test]
    fn huge_write_promotes_immediately() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        assert_eq!(
            p.on_write(PROMOTE_IMMEDIATE_THRESHOLD, INT, now),
            Some(LaneClass::Bulk)
        );
    }

    #[test]
    fn promote_respects_cooldown() {
        let mut p = LanePolicy::new();
        let t0 = Instant::now();
        p.note_migration(t0);
        for _ in 0..HISTORY_MIN {
            assert_eq!(p.on_write(3000, INT, t0 + Duration::from_millis(10)), None);
        }
        assert_eq!(
            p.on_write(
                PROMOTE_IMMEDIATE_THRESHOLD,
                INT,
                t0 + Duration::from_millis(10)
            ),
            None
        );
        let later = t0 + MIGRATION_COOLDOWN + Duration::from_millis(1);
        assert_eq!(p.on_write(3000, INT, later), Some(LaneClass::Bulk));
    }

    #[test]
    fn no_promote_on_small_writes() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..10 {
            assert_eq!(p.on_write(100, INT, now), None);
        }
    }

    #[test]
    fn coalescing_artifacts_do_not_flap() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..64 {
            assert_eq!(p.on_write(300, INT, now), None);
            assert_eq!(p.on_write(300, INT, now), None);
            assert_eq!(p.on_write(4096, INT, now), None);
            assert_eq!(p.on_write(300, INT, now), None);
        }
    }

    #[test]
    fn demote_after_streak_first_migration_immediate() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(p.on_write(100, BULK, now), None);
        }
        assert_eq!(p.on_write(100, BULK, now), Some(LaneClass::Interactive));
    }

    #[test]
    fn demote_respects_cooldown_then_fires() {
        let mut p = LanePolicy::new();
        let t0 = Instant::now();
        p.note_migration(t0);
        for _ in 0..DEMOTE_STREAK + 2 {
            assert_eq!(p.on_write(100, BULK, t0 + Duration::from_millis(10)), None);
        }
        let later = t0 + MIGRATION_COOLDOWN + Duration::from_millis(1);
        assert_eq!(p.on_write(100, BULK, later), Some(LaneClass::Interactive));
    }

    #[test]
    fn demote_blocked_while_history_reads_bulk() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..HISTORY_MAX {
            assert_eq!(p.on_write(3000, BULK, now), None);
        }
        for _ in 0..DEMOTE_STREAK {
            assert_eq!(p.on_write(100, BULK, now), None);
        }
        let mut demoted = false;
        for _ in 0..2 * HISTORY_MAX {
            if p.on_write(100, BULK, now) == Some(LaneClass::Interactive) {
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
        assert_eq!(p.on_write(50_000, None, now), None);
    }

    #[test]
    fn large_write_resets_demote_streak() {
        let mut p = LanePolicy::new();
        let now = Instant::now();
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(p.on_write(100, BULK, now), None);
        }
        assert_eq!(p.on_write(3000, BULK, now), None);
        for _ in 0..DEMOTE_STREAK - 1 {
            assert_eq!(p.on_write(100, BULK, now), None);
        }
    }
}
