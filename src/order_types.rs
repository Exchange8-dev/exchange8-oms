// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS order types & algorithms.
//!
//! - **Conditional/OCO/bracket**: one-cancels-other groups - a leg
//!   triggering (or filling) cancels its siblings; brackets are
//!   take-profit + stop-loss pairs around a parent position.
//! - **Multi-leg (complex) orders**: atomic all-or-none across legs
//!   (options spreads) - a leg book tracks per-leg fillable qty; the
//!   order executes only when every leg can fill simultaneously.
//! - **TWAP/VWAP/POV algos**: slice schedulers emitting child orders
//!   (OMS-level, per design §3 of the order-types doc).
//! - **MOC/MOO**: auction participation orders - queued to the
//!   auction uncross, never book-resting.
//! - **Iceberg slicing**: reserve accounting + slice re-arm.
//! - **Quote cache**: per-instrument BBO/EBBO snapshots driving peg
//!   re-evaluation.
//! - **ME id sequence lease + idempotency**: child-order id leasing
//!   with dedup on client idempotency keys.
//!
//! Design: `design/sentin8-exchange8-order-types-technical-design.md` §2-§3,
//! `design/sentin8-exchange8-oms-technical-design.md` §4.

use std::collections::HashMap;

use exchange8::fixed::{Price, Qty};

// ------------------------------------------------------------------------
// OCO groups & brackets
// ------------------------------------------------------------------------

/// OCO group semantics.
#[derive(Clone, Debug, PartialEq)]
pub struct OcoLeg {
    pub order_id: u64,
    /// Trigger/limit terms (reuse ConditionalOrder in triggers.rs at the
    /// shell; here the pure group logic is id-driven).
    pub kind: OcoKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OcoKind {
    /// Take-profit leg.
    TakeProfit,
    /// Stop-loss leg.
    StopLoss,
}

/// One-cancels-other group: first leg to fire cancels the sibling.
pub struct OcoGroup {
    legs: Vec<OcoLeg>,
    /// Set when one leg fired - group terminal.
    fired: Option<u64>,
}

impl OcoGroup {
    pub fn new(legs: Vec<OcoLeg>) -> Self {
        OcoGroup { legs, fired: None }
    }

    /// A leg triggered/filled: it executes, siblings cancel.
    /// Returns (executed_id, cancelled_ids).
    pub fn on_fire(&mut self, order_id: u64) -> (u64, Vec<u64>) {
        if self.fired.is_some() {
            // Already terminal - no-op (idempotent re-fire).
            return (0, Vec::new());
        }
        if !self.legs.iter().any(|l| l.order_id == order_id) {
            return (0, Vec::new());
        }
        self.fired = Some(order_id);
        let cancelled: Vec<u64> = self
            .legs
            .iter()
            .map(|l| l.order_id)
            .filter(|id| *id != order_id)
            .collect();
        (order_id, cancelled)
    }

    /// Bracket helper: take-profit above (long) / stop-loss below.
    pub fn bracket(parent_id: u64, tp_id: u64, sl_id: u64) -> Self {
        OcoGroup::new(vec![
            OcoLeg {
                order_id: tp_id,
                kind: OcoKind::TakeProfit,
            },
            OcoLeg {
                order_id: sl_id,
                kind: OcoKind::StopLoss,
            },
        ])
        .with_parent(parent_id)
    }

    fn with_parent(self, _parent_id: u64) -> Self {
        // Parent linkage is a shell concern (ledger join); the pure group
        // only needs leg ids.
        self
    }

    pub fn is_terminal(&self) -> bool {
        self.fired.is_some()
    }
}

/// Registry of OCO groups.
pub struct OcoRegistry {
    groups: Vec<OcoGroup>,
    /// order_id → group index.
    index: HashMap<u64, usize>,
}

impl OcoRegistry {
    pub fn new() -> Self {
        OcoRegistry {
            groups: Vec::new(),
            index: HashMap::new(),
        }
    }

    pub fn register(&mut self, group: OcoGroup) {
        let idx = self.groups.len();
        for leg in &group.legs {
            self.index.insert(leg.order_id, idx);
        }
        self.groups.push(group);
    }

    /// A leg fired: executes + cancels siblings (OCO semantics).
    pub fn on_fire(&mut self, order_id: u64) -> (u64, Vec<u64>) {
        let Some(&idx) = self.index.get(&order_id) else {
            return (0, Vec::new());
        };
        self.groups[idx].on_fire(order_id)
    }

    /// External cancel of one leg (client request): siblings also cancel
    /// (bracket semantics - a broken bracket is fully withdrawn).
    pub fn on_cancel(&mut self, order_id: u64) -> Vec<u64> {
        let Some(&idx) = self.index.get(&order_id) else {
            return Vec::new();
        };
        let ids: Vec<u64> = self.groups[idx].legs.iter().map(|l| l.order_id).collect();
        self.groups[idx].fired = Some(0); // terminal
        ids
    }
}

impl Default for OcoRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------------
// Multi-leg atomic orders
// ------------------------------------------------------------------------

/// One leg of a complex order.
#[derive(Clone, Debug, PartialEq)]
pub struct Leg {
    pub instrument_id: u32,
    pub side_bid: bool,
    pub qty: Qty,
    /// Limit for the leg (None = market leg at uncross).
    pub limit: Option<Price>,
    /// Fillable qty found on the opposite book right now.
    pub fillable: Qty,
}

/// Atomic multi-leg evaluation (design §2: "all legs fill or none").
#[derive(Clone, Debug, PartialEq)]
pub struct ComplexOrder {
    pub order_id: u64,
    pub legs: Vec<Leg>,
}

impl ComplexOrder {
    /// True when every leg can fill its full qty simultaneously at limits.
    pub fn all_fillable(&self) -> bool {
        self.legs.iter().all(|l| l.fillable.raw >= l.qty.raw)
    }

    /// Per-leg executable qty if fired now (full qty when atomic gate
    /// passes, else nothing).
    pub fn evaluate(&self) -> Option<Vec<(u32, Qty)>> {
        if self.all_fillable() {
            Some(self.legs.iter().map(|l| (l.instrument_id, l.qty)).collect())
        } else {
            None
        }
    }

    /// Atomic fillability at limits: every leg fillable ≥ qty (the
    /// caller supplies fillable at the *limit price or better*).
    pub fn within_limits(&self) -> bool {
        self.all_fillable()
    }
}

// ------------------------------------------------------------------------
// TWAP/VWAP/POV slice schedulers
// ------------------------------------------------------------------------

/// Algo kind (design §3 table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlgoKind {
    /// Uniform slices over the horizon.
    Twap,
    /// Volume-profile-weighted slices.
    Vwap,
    /// Percentage of realtime volume.
    Pov { percent: u32 },
}

/// An algo child order to emit.
#[derive(Clone, Debug, PartialEq)]
pub struct AlgoSlice {
    pub parent_id: u64,
    pub slice_no: u32,
    pub qty: Qty,
}

/// Algo state: parent qty, horizon, emitted slices.
pub struct AlgoEngine {
    kind: AlgoKind,
    parent_id: u64,
    total: Qty,
    /// Slice interval count over the horizon.
    intervals: u32,
    /// Intervals already fired.
    fired: u32,
    /// Remaining qty.
    remaining: Qty,
    /// Per-interval volume profile (VWAP); uniform when empty.
    profile: Vec<u64>,
}

impl AlgoEngine {
    /// TWAP: uniform split; remainder distributed one-per-interval front.
    pub fn twap(parent_id: u64, total: Qty, intervals: u32) -> Self {
        AlgoEngine {
            kind: AlgoKind::Twap,
            parent_id,
            total,
            intervals: intervals.max(1),
            fired: 0,
            remaining: total,
            profile: Vec::new(),
        }
    }

    /// VWAP: `profile` is per-interval expected venue volume (nonzero).
    pub fn vwap(parent_id: u64, total: Qty, profile: Vec<u64>) -> Self {
        let intervals = profile.len().max(1) as u32;
        AlgoEngine {
            kind: AlgoKind::Vwap,
            parent_id,
            total,
            intervals,
            fired: 0,
            remaining: total,
            profile,
        }
    }

    /// POV: each interval emits `percent` of the interval's volume (caller
    /// supplies realized volume per tick via `next_pov_slice`).
    pub fn pov(parent_id: u64, total: Qty, percent: u32) -> Self {
        AlgoEngine {
            kind: AlgoKind::Pov { percent },
            parent_id,
            total,
            intervals: 0, // open-ended until total exhausted
            fired: 0,
            remaining: total,
            profile: Vec::new(),
        }
    }

    /// Next scheduled slice (TWAP/VWAP interval tick). None = done.
    pub fn next_scheduled_slice(&mut self) -> Option<AlgoSlice> {
        if self.remaining.raw == 0 {
            return None;
        }
        let intervals_left = self.intervals.saturating_sub(self.fired);
        if intervals_left == 0 {
            return None;
        }
        let slice_qty = match self.kind {
            AlgoKind::Twap => {
                // Uniform with remainder spread over remaining intervals.
                let base = self.remaining.raw / intervals_left as u64;
                let extra = (self.remaining.raw % intervals_left as u64).min(1);
                Qty::units(base + extra)
            }
            AlgoKind::Vwap => {
                // Weight = this interval's profile share of the profile tail.
                let start = self.fired as usize;
                let tail: u64 = self.profile[start..].iter().sum();
                let w = *self.profile.get(start).unwrap_or(&1);
                let base = if tail == 0 {
                    self.remaining.raw / intervals_left as u64
                } else {
                    self.remaining.raw * w / tail.max(1)
                };
                let extra = (self.remaining.raw.saturating_sub(
                    (self.remaining.raw / intervals_left as u64) * intervals_left as u64,
                ))
                .min(1);
                Qty::units(base.max(1) + extra.min(1)).min(self.remaining)
            }
            AlgoKind::Pov { .. } => return None, // POV uses next_pov_slice
        };
        let qty = slice_qty.min(self.remaining);
        self.remaining = Qty::units(self.remaining.raw - qty.raw);
        self.fired += 1;
        Some(AlgoSlice {
            parent_id: self.parent_id,
            slice_no: self.fired,
            qty,
        })
    }

    /// POV: participate with `percent` of the interval's realized volume.
    pub fn next_pov_slice(&mut self, interval_volume: u64) -> Option<AlgoSlice> {
        if self.remaining.raw == 0 {
            return None;
        }
        let AlgoKind::Pov { percent } = self.kind else {
            return None;
        };
        let target = interval_volume * percent as u64 / 100;
        let qty = Qty::units(target).min(self.remaining);
        if qty.raw == 0 {
            return None;
        }
        self.remaining = Qty::units(self.remaining.raw - qty.raw);
        self.fired += 1;
        Some(AlgoSlice {
            parent_id: self.parent_id,
            slice_no: self.fired,
            qty,
        })
    }

    pub fn remaining(&self) -> Qty {
        self.remaining
    }

    pub fn done(&self) -> bool {
        self.remaining.raw == 0
    }
}

// ------------------------------------------------------------------------
// MOC/MOO auction participation
// ------------------------------------------------------------------------

/// Auction participation order kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuctionKind {
    /// Market-on-open.
    Moo,
    /// Market-on-close.
    Moc,
    /// Limit-on-open.
    Loo { limit: Price },
    /// Limit-on-close.
    Loc { limit: Price },
}

/// Auction queue: orders held for the uncross (never book-resting).
pub struct AuctionQueue {
    kind: AuctionKind,
    /// (order_id, qty, side) - market-style imbalance contribution.
    orders: Vec<(u64, Qty, bool)>,
}

impl AuctionQueue {
    pub fn new(kind: AuctionKind) -> Self {
        AuctionQueue {
            kind,
            orders: Vec::new(),
        }
    }

    pub fn enqueue(&mut self, order_id: u64, qty: Qty, side_bid: bool) {
        self.orders.push((order_id, qty, side_bid));
    }

    /// Imbalance (bid qty − ask qty) - feeds the auction indicative feed.
    pub fn imbalance(&self) -> i128 {
        self.orders
            .iter()
            .map(|(_, q, bid)| {
                if *bid {
                    q.raw as i128
                } else {
                    -(q.raw as i128)
                }
            })
            .sum()
    }

    /// Drain for the uncross (auction engine consumes).
    pub fn drain(&mut self) -> Vec<(u64, Qty, bool)> {
        std::mem::take(&mut self.orders)
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn kind(&self) -> AuctionKind {
        self.kind
    }
}

// ------------------------------------------------------------------------
// Iceberg slicing + reserve accounting
// ------------------------------------------------------------------------

/// Iceberg state: displayed slice + hidden reserve, re-arm on fill.
pub struct IcebergState {
    pub order_id: u64,
    /// Total remaining (displayed + reserve).
    pub remaining: Qty,
    /// Max displayed per slice.
    pub max_floor: Qty,
    /// Currently displayed.
    pub displayed: Qty,
    /// Slice counter (sub-order id feed identity).
    pub slice_count: u32,
}

impl IcebergState {
    pub fn new(order_id: u64, total: Qty, max_floor: Qty) -> Self {
        let displayed = total.min(max_floor);
        IcebergState {
            order_id,
            remaining: total,
            max_floor,
            displayed,
            slice_count: 1,
        }
    }

    /// A fill of `qty` against the displayed slice: reduce, then re-arm a
    /// new slice from reserve (moves to FIFO tail at the ME).
    /// Returns the new displayed qty when a new slice arms, else 0 when
    /// the iceberg is done.
    pub fn on_fill(&mut self, qty: Qty) -> Qty {
        self.remaining = Qty::units(self.remaining.raw.saturating_sub(qty.raw));
        self.displayed = Qty::units(self.displayed.raw.saturating_sub(qty.raw));
        if self.remaining.raw == 0 {
            self.displayed = Qty::ZERO;
            return Qty::ZERO;
        }
        if self.displayed.raw == 0 {
            // Re-arm from reserve.
            self.displayed = self.remaining.min(self.max_floor);
            self.slice_count += 1;
        }
        self.displayed
    }

    /// Cancel the displayed slice (reserve stays until done or killed).
    pub fn on_slice_cancel(&mut self) -> Qty {
        let was = self.displayed;
        self.displayed = Qty::ZERO;
        was
    }

    pub fn reserve(&self) -> Qty {
        Qty::units(self.remaining.raw.saturating_sub(self.displayed.raw))
    }
}

// ------------------------------------------------------------------------
// Quote cache (per-instrument BBO/EBBO snapshots)
// ------------------------------------------------------------------------

/// Cached quote snapshot per instrument.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QuoteSnapshot {
    pub bid: Option<Price>,
    pub ask: Option<Price>,
    /// EBBO sources (venue MICs) when consolidated.
    pub ebbo: bool,
    pub ts_us: u64,
}

/// The quote cache: BBO/EBBO in, re-evaluation set out.
pub struct QuoteCache {
    quotes: HashMap<u32, QuoteSnapshot>,
    /// Instruments whose quote changed since the last drain (peg
    /// re-evaluation set).
    dirty: Vec<u32>,
}

impl QuoteCache {
    pub fn new() -> Self {
        QuoteCache {
            quotes: HashMap::new(),
            dirty: Vec::new(),
        }
    }

    /// Update one instrument's quote; marks it dirty on change.
    pub fn on_quote(&mut self, instrument_id: u32, q: QuoteSnapshot) {
        let changed = self
            .quotes
            .get(&instrument_id)
            .map(|old| old.bid != q.bid || old.ask != q.ask || old.ebbo != q.ebbo)
            .unwrap_or(true);
        if changed {
            self.dirty.push(instrument_id);
        }
        self.quotes.insert(instrument_id, q);
    }

    /// Drain the re-evaluation set (peg supervisor re-prices these).
    pub fn take_dirty(&mut self) -> Vec<u32> {
        let mut d = std::mem::take(&mut self.dirty);
        d.sort_unstable();
        d.dedup();
        d
    }

    pub fn quote(&self, instrument_id: u32) -> Option<QuoteSnapshot> {
        self.quotes.get(&instrument_id).copied()
    }
}

impl Default for QuoteCache {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------------
// Child-order id lease + idempotency
// ------------------------------------------------------------------------

/// Child-order id leaser (blocks leased from the platform sequence facility).
pub struct ChildIdLease {
    next: u64,
    end: u64,
}

impl ChildIdLease {
    pub fn new(from: u64, batch: u64) -> Self {
        ChildIdLease {
            next: from,
            end: from.saturating_add(batch),
        }
    }

    pub fn next_id(&mut self) -> Option<u64> {
        if self.next >= self.end {
            return None;
        }
        let id = self.next;
        self.next += 1;
        Some(id)
    }

    pub fn refill(&mut self, from: u64, batch: u64) {
        self.next = from;
        self.end = from.saturating_add(batch);
    }
}

/// Idempotency dedup on client keys (see DESIGN.md §6).
pub struct IdempotencyGate {
    seen: HashMap<String, u64>,
}

impl IdempotencyGate {
    pub fn new() -> Self {
        IdempotencyGate {
            seen: HashMap::new(),
        }
    }

    /// Register a client idempotency key → assigned child id. Returns the
    /// previously assigned id on duplicate (exactly-once semantics).
    pub fn register(&mut self, key: &str, assigned_id: u64) -> u64 {
        *self.seen.entry(key.to_string()).or_insert(assigned_id)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

impl Default for IdempotencyGate {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oco_first_leg_fires_cancels_sibling() {
        let mut reg = OcoRegistry::new();
        reg.register(OcoGroup::bracket(1, 10, 11));
        // Take-profit fires.
        let (exec, cancelled) = reg.on_fire(10);
        assert_eq!(exec, 10);
        assert_eq!(cancelled, vec![11]);
        // Re-fire of the cancelled leg is a no-op (terminal group).
        assert_eq!(reg.on_fire(11), (0, vec![]));
    }

    #[test]
    fn oco_external_cancel_breaks_whole_bracket() {
        let mut reg = OcoRegistry::new();
        reg.register(OcoGroup::bracket(1, 10, 11));
        let cancelled = reg.on_cancel(10);
        assert_eq!(cancelled, vec![10, 11]);
    }

    #[test]
    fn oco_unknown_order_is_noop() {
        let mut reg = OcoRegistry::new();
        reg.register(OcoGroup::bracket(1, 10, 11));
        assert_eq!(reg.on_fire(99), (0, vec![]));
        assert!(reg.on_cancel(99).is_empty());
    }

    #[test]
    fn complex_order_atomic_gate() {
        let mk = |fillable_a: u64, fillable_b: u64| ComplexOrder {
            order_id: 1,
            legs: vec![
                Leg {
                    instrument_id: 10,
                    side_bid: true,
                    qty: Qty::units(100),
                    limit: Some(Price::equity(10_000)),
                    fillable: Qty::units(fillable_a),
                },
                Leg {
                    instrument_id: 11,
                    side_bid: false,
                    qty: Qty::units(100),
                    limit: Some(Price::equity(10_000)),
                    fillable: Qty::units(fillable_b),
                },
            ],
        };
        // Both legs fillable → atomic pass.
        assert!(mk(150, 120).evaluate().is_some());
        // One leg short → nothing (atomic).
        assert!(mk(150, 80).evaluate().is_none());
        assert!(mk(50, 120).evaluate().is_none());
    }

    #[test]
    fn twap_splits_uniformly_and_finishes() {
        let mut twap = AlgoEngine::twap(1, Qty::units(1_000), 10);
        let mut total = 0u64;
        let mut slices = 0;
        while let Some(s) = twap.next_scheduled_slice() {
            total += s.qty.raw;
            slices += 1;
            assert_eq!(s.parent_id, 1);
        }
        assert_eq!(slices, 10);
        assert_eq!(total, 1_000);
        assert!(twap.done());
    }

    #[test]
    fn twap_uneven_split_still_conserves() {
        // 1005 over 10 intervals: 101×5 + 100×5 via the +1 remainder.
        let mut twap = AlgoEngine::twap(1, Qty::units(1_005), 10);
        let mut total = 0;
        while let Some(s) = twap.next_scheduled_slice() {
            total += s.qty.raw;
        }
        assert_eq!(total, 1_005);
    }

    #[test]
    fn vwap_weights_by_profile() {
        // Profile 3:1 → first slice is ~3/4 of total.
        let mut vwap = AlgoEngine::vwap(1, Qty::units(400), vec![300, 100]);
        let s1 = vwap.next_scheduled_slice().unwrap();
        assert_eq!(s1.qty.raw, 300); // 400 × 300/400
        let s2 = vwap.next_scheduled_slice().unwrap();
        assert_eq!(s2.qty.raw, 100);
        assert!(vwap.done());
    }

    #[test]
    fn pov_participates_by_volume() {
        let mut pov = AlgoEngine::pov(1, Qty::units(500), 25);
        // Interval volume 800 → 25% = 200.
        let s1 = pov.next_pov_slice(800).unwrap();
        assert_eq!(s1.qty.raw, 200);
        // Interval volume 1_200 → 300 (capped by remaining 300).
        let s2 = pov.next_pov_slice(1_200).unwrap();
        assert_eq!(s2.qty.raw, 300);
        assert!(pov.done());
        assert!(pov.next_pov_slice(1_000).is_none());
    }

    #[test]
    fn auction_queue_imbalance_and_drain() {
        let mut q = AuctionQueue::new(AuctionKind::Moc);
        q.enqueue(1, Qty::units(500), true);
        q.enqueue(2, Qty::units(300), true);
        q.enqueue(3, Qty::units(400), false);
        assert_eq!(q.imbalance(), 400); // 800 bid − 400 ask
        assert_eq!(q.len(), 3);
        let drained = q.drain();
        assert_eq!(drained.len(), 3);
        assert!(q.is_empty());
        assert_eq!(q.imbalance(), 0);
    }

    #[test]
    fn iceberg_rearms_slices_from_reserve() {
        let mut ice = IcebergState::new(1, Qty::units(300), Qty::units(100));
        assert_eq!(ice.displayed.raw, 100);
        assert_eq!(ice.reserve().raw, 200);
        // Fill the displayed slice fully → re-arm.
        let d = ice.on_fill(Qty::units(100));
        assert_eq!(d.raw, 100);
        assert_eq!(ice.slice_count, 2);
        assert_eq!(ice.reserve().raw, 100);
        // Partial fill 40 → displayed 60 stays.
        let d2 = ice.on_fill(Qty::units(40));
        assert_eq!(d2.raw, 60);
        assert_eq!(ice.slice_count, 2);
        // Kill the slice → remaining 160 all in reserve, nothing displayed.
        let was = ice.on_slice_cancel();
        assert_eq!(was.raw, 60);
        assert_eq!(ice.reserve().raw, 160);
        assert_eq!(ice.displayed.raw, 0);
    }

    #[test]
    fn iceberg_final_fill_terminates() {
        let mut ice = IcebergState::new(1, Qty::units(100), Qty::units(100));
        assert_eq!(ice.on_fill(Qty::units(100)).raw, 0);
        assert_eq!(ice.remaining.raw, 0);
        assert_eq!(ice.slice_count, 1); // no re-arm on done
    }

    #[test]
    fn quote_cache_marks_dirty_on_change_only() {
        let mut qc = QuoteCache::new();
        let snap = QuoteSnapshot {
            bid: Some(Price::equity(9_900)),
            ask: Some(Price::equity(10_100)),
            ebbo: false,
            ts_us: 1,
        };
        qc.on_quote(3, snap);
        assert_eq!(qc.take_dirty(), vec![3]);
        // Same quote again → not dirty.
        qc.on_quote(3, snap);
        assert!(qc.take_dirty().is_empty());
        // Price change → dirty.
        qc.on_quote(
            3,
            QuoteSnapshot {
                bid: Some(Price::equity(9_950)),
                ask: Some(Price::equity(10_100)),
                ebbo: false,
                ts_us: 2,
            },
        );
        assert_eq!(qc.take_dirty(), vec![3]);
    }

    #[test]
    fn child_id_lease_and_idempotency() {
        let mut lease = ChildIdLease::new(1, 2);
        let mut gate = IdempotencyGate::new();
        let id1 = lease.next_id().unwrap();
        // First registration returns the assigned id.
        assert_eq!(gate.register("client-key-1", id1), id1);
        // Duplicate with a *different* candidate id → original returned.
        assert_eq!(gate.register("client-key-1", 99), id1);
        let id2 = lease.next_id().unwrap();
        assert_eq!(gate.register("client-key-2", id2), id2);
        assert_eq!(gate.len(), 2);
        assert!(lease.next_id().is_none());
        lease.refill(3, 1);
        assert_eq!(lease.next_id(), Some(3));
    }
}
