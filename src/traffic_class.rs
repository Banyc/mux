use crate::fair_queue;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

const LANE_HELLO_INTERACTIVE: u8 = 0xD1;
const LANE_HELLO_BULK: u8 = 0xD2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneClass {
    Interactive,
    Bulk,
}

impl LaneClass {
    pub(crate) fn hello_byte(self) -> u8 {
        match self {
            LaneClass::Interactive => LANE_HELLO_INTERACTIVE,
            LaneClass::Bulk => LANE_HELLO_BULK,
        }
    }
    pub(crate) fn from_hello_byte(b: u8) -> Option<Self> {
        match b {
            LANE_HELLO_INTERACTIVE => Some(LaneClass::Interactive),
            LANE_HELLO_BULK => Some(LaneClass::Bulk),
            _ => None,
        }
    }
}

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
pub(crate) struct SizeMix {
    small_count: u32,
    bulk_count: u32,
}
impl SizeMix {
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
    classifier: SizeMix,
    small_streak: usize,
    last_migration: Option<Instant>,
}
impl LanePolicy {
    pub(crate) fn new() -> Self {
        Self {
            classifier: SizeMix::new(),
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

/// A stream is relegated from latency-sensitive (the default, protected)
/// to bulk once more than one third of its recent sends were over
/// [`BULK_THRESHOLD`] and it has been idle for this long.
pub(crate) const LATENCY_IDLE: Duration = Duration::from_secs(30);

/// Minimum number of sends before the bulk ratio is evaluated.
/// Maximum send history kept per stream. Once the counters reach this size
/// they are halved before tallying the new observation, so classification
/// tracks recent behaviour and a bulk stream reverts to latency-sensitive
/// within a bounded number of small sends.
#[cfg(test)]
pub(crate) const LATENCY_HISTORY_MAX: usize = crate::traffic_class::HISTORY_MAX;

/// Per-stream traffic class, keyed by fair-queue token.
///
/// - Latency-sensitive (default): the stream is protected. While any
///   open stream is sensitive, every Data dispatch is capped at a small
///   cap so a later-arriving small stream can preempt the tail.
/// - Bulk: the stream has been relegated. Only when every open stream is
///   bulk does a dispatch use the bulk cap for throughput.
///
/// The class is computed on demand from per-stream send observations (recent
/// send sizes and last-sent time); it is not stored as a field. Global
/// sensitivity is cheap to query in the common case: `bulk_count` and
/// `next_bulk_transition` are maintained incrementally, and
/// `any_latency_sensitive` is O(1) except when a time-driven transition is due
/// (then it recomputes aggregates, O(open_count), and resets the timer).
#[derive(Debug)]
pub(crate) struct LatencyControl {
    /// Send history per open stream token. `Small`/`Bulk` are tallied
    /// incrementally so the bulk ratio is a cheap division, and
    /// `last_sent` lets a bulk stream revert to latency-sensitive if it
    /// resumes after the idle window.
    streams: HashMap<fair_queue::QueueToken, SizeMix>,
    /// Number of currently-open streams (`Open` seen, no `Close`/`Fin` yet).
    open_count: usize,
    /// Number of open streams currently classified bulk.
    bulk_count: usize,
    /// Earliest time at which some non-bulk stream may transition to bulk
    /// (its `last_sent + LATENCY_IDLE`). `None` when no non-bulk stream could
    /// ever transition (e.g. no history yet). `any_latency_sensitive` is O(1)
    /// while `now < next_bulk_transition`; once `now` crosses it, aggregates
    /// are recomputed and this is reset.
    next_bulk_transition: Option<Instant>,
}

impl LatencyControl {
    pub(crate) fn new() -> Self {
        Self {
            streams: HashMap::new(),
            open_count: 0,
            bulk_count: 0,
            next_bulk_transition: None,
        }
    }
    pub(crate) fn open(&mut self, token: fair_queue::QueueToken, now: Instant) {
        let obs = SizeMix::new();
        self.open_count += 1;
        if obs.is_bulk() {
            self.bulk_count += 1;
        } else {
            self.next_bulk_transition = Some(now + LATENCY_IDLE);
        }
        self.streams.insert(token, obs);
    }
    pub(crate) fn close(&mut self, token: fair_queue::QueueToken) {
        let Some(obs) = self.streams.remove(&token) else {
            return;
        };
        if obs.is_bulk() {
            self.bulk_count = self.bulk_count.strict_sub(1);
        }
        self.open_count = self.open_count.strict_sub(1);
    }
    pub(crate) fn record_send(&mut self, token: fair_queue::QueueToken, size: usize, now: Instant) {
        let Some(obs) = self.streams.get_mut(&token) else {
            return;
        };
        let was_bulk = obs.is_bulk();
        obs.record(size);
        let is_bulk = obs.is_bulk();
        if was_bulk != is_bulk {
            if is_bulk {
                self.bulk_count += 1;
            } else {
                self.bulk_count = self.bulk_count.strict_sub(1);
            }
        }
        if !is_bulk {
            self.next_bulk_transition = Some(now + LATENCY_IDLE);
        }
    }
    pub fn any_latency_sensitive(&self) -> bool {
        if self.open_count == self.bulk_count {
            return false;
        }
        self.next_bulk_transition.is_none_or(|t| Instant::now() < t)
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
        let mut c = SizeMix::new();
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
        let mut c = SizeMix::new();
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
