// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS trigger engine - stop/stop-limit, trailing stops, OCO/OCA groups.
//!
//! Stop and conditional orders are held here (design: order-types doc §2) and
//! released to Exchange8:ME when the trigger fires; they never rest in the ME book.
//! Pure evaluation: the shell feeds market ticks (last/bid/ask from MD/VFeeds)
//! and index publications, and order lifecycle events; the engine returns
//! trigger/release/cancel actions.
//!
//! Reference surfaces (DESIGN.md §4 - `oms.conditional_order.condition_type`):
//! - `stop_last` / `stop_bid` / `stop_ask` - instrument quote references.
//! - `stop_index` - the trigger watches an **index publication** (the
//!   `sentin8-exchange8-index-engine-technical-design.md` publication the
//!   index engine emits), not the instrument's own quote; fed via
//!   [`TriggerEngine::on_index`].
//! - `trailing_stop` - the trailed quote reference (ratchet, never loosen).
//! - `oco` - one-cancels-other: the first leg to fire releases, siblings
//!   cancel (the standard bracket semantics).
//! - `oca` - one-cancels-all: a group of INDEPENDENT orders that share a
//!   lifecycle - the first leg to fire releases it AND cancels all group
//!   siblings (like OCO), but any member's client CANCEL also withdraws the
//!   whole group even when it never fired (the "cancel-all" semantics an OCO
//!   bracket already has; OCA's distinction is that the members are
//!   unrelated orders entered independently, not a constructed bracket -
//!   group identity is the durable `oms.conditional_order.oco_group` key).
//!
//! Design: `design/sentin8-exchange8-order-types-technical-design.md` §2.

/// Which reference price a trigger watches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerRef {
    /// Last traded price .
    Last,
    /// Best bid (for sell stops / buy-trigger reference).
    Bid,
    /// Best ask (for buy stops / sell-trigger reference).
    Ask,
    /// An index publication (the index engine's published level) - the
    /// `stop_index` condition type; the trigger compares the published
    /// index value against `trigger_price`.
    Index,
}

/// A held conditional order awaiting its trigger.
#[derive(Debug, Clone)]
pub struct ConditionalOrder {
    pub order_id: u64,
    pub instrument_id: u32,
    /// Buy/sell of the order to release on trigger.
    pub is_buy: bool,
    pub trigger_ref: TriggerRef,
    /// Trigger threshold in raw ticks.
    pub trigger_price: i128,
    /// Trailing offset in raw ticks (0 = plain stop).
    pub trail_offset: i128,
    /// Released order type: None = market, Some = limit price.
    pub limit_price: Option<i128>,
    /// OCO/OCA group (one-cancels-other / one-cancels-all).
    pub oco_group: Option<u64>,
    /// Best trail reference seen so far (for trailing stops).
    trail_extreme: Option<i128>,
}

impl ConditionalOrder {
    /// New plain stop order.
    pub fn stop(
        order_id: u64,
        instrument_id: u32,
        is_buy: bool,
        trigger_ref: TriggerRef,
        trigger_price: i128,
        limit_price: Option<i128>,
    ) -> Self {
        ConditionalOrder {
            order_id,
            instrument_id,
            is_buy,
            trigger_ref,
            trigger_price,
            trail_offset: 0,
            limit_price,
            oco_group: None,
            trail_extreme: None,
        }
    }

    /// New trailing stop: trigger trails the reference by `offset` ticks.
    pub fn trailing(
        order_id: u64,
        instrument_id: u32,
        is_buy: bool,
        trigger_ref: TriggerRef,
        initial_trigger: i128,
        offset: i128,
        limit_price: Option<i128>,
    ) -> Self {
        ConditionalOrder {
            order_id,
            instrument_id,
            is_buy,
            trigger_ref,
            trigger_price: initial_trigger,
            trail_offset: offset,
            limit_price,
            oco_group: None,
            trail_extreme: None,
        }
    }

    pub fn in_group(mut self, group: u64) -> Self {
        self.oco_group = Some(group);
        self
    }

    /// Evaluate a market tick for this order. Returns the action for this
    /// single order; group cancellation is resolved by the `TriggerEngine`.
    pub fn evaluate(&mut self, tick: &MarketTick) -> Option<TriggerAction> {
        if tick.instrument_id != self.instrument_id {
            return None;
        }
        let reference = match self.trigger_ref {
            TriggerRef::Last => tick.last,
            TriggerRef::Bid => tick.bid,
            TriggerRef::Ask => tick.ask,
            // A quote-referenced trigger never evaluates on an index
            // publication (and vice versa - the quote path skips
            // index-linked orders under `on_tick`).
            TriggerRef::Index => None,
        }?;
        self.evaluate_reference(reference)
    }

    /// Evaluate one reference price (the shared trigger math for quote
    /// references and the index publication).
    fn evaluate_reference(&mut self, reference: i128) -> Option<TriggerAction> {
        // Trailing stop: ratchet the trigger to trail the best reference seen.
        if self.trail_offset > 0 {
            let prev_extreme = self.trail_extreme.unwrap_or(reference);
            let extreme = if self.is_buy {
                // Buy trailing stop trails DOWN with falling prices (stop
                // sits below the lowest reference seen): extreme = min.
                prev_extreme.min(reference)
            } else {
                // Sell trailing stop trails UP: extreme = max.
                prev_extreme.max(reference)
            };
            self.trail_extreme = Some(extreme);
            let trailed = if self.is_buy {
                // Buy trailing stop sits ABOVE the trough (fires on the way back up).
                extreme + self.trail_offset
            } else {
                // Sell trailing stop sits BELOW the peak (fires on the way back down).
                extreme - self.trail_offset
            };
            // Only ratchet in the protective direction (never loosen).
            if self.is_buy && trailed < self.trigger_price {
                self.trigger_price = trailed;
            }
            if !self.is_buy && trailed > self.trigger_price {
                self.trigger_price = trailed;
            }
        }

        // Trigger condition: buy stops fire at/above the stop; sell stops at/below.
        let fired = if self.is_buy {
            reference >= self.trigger_price
        } else {
            reference <= self.trigger_price
        };
        fired.then_some(TriggerAction::Release {
            order_id: self.order_id,
            is_buy: self.is_buy,
            limit_price: self.limit_price,
            triggered_at: reference,
        })
    }
}

/// Market tick inputs (raw ticks; None = no quote for that field).
#[derive(Clone, Copy, Debug, Default)]
pub struct MarketTick {
    pub instrument_id: u32,
    pub last: Option<i128>,
    pub bid: Option<i128>,
    pub ask: Option<i128>,
}

/// An index publication (the index engine's published level - market-data
/// design §index; the value feeds `stop_index` triggers). Raw micros-level
/// fixed-point value (scaled ×10⁴ to the equity price scale).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexPublication {
    /// The index id (its own id space; held orders key on it).
    pub index_id: u32,
    /// The published index value (raw scale).
    pub value: i128,
    /// Publication timestamp, microseconds since the Unix epoch.
    pub ts_us: u64,
}

/// Trigger-engine outcome for one order.
#[derive(Debug, PartialEq, Eq)]
pub enum TriggerAction {
    /// Trigger fired: release the order to Exchange8:ME.
    Release {
        order_id: u64,
        is_buy: bool,
        limit_price: Option<i128>,
        triggered_at: i128,
    },
    /// Cancelled because its OCO/OCA sibling fired or was cancelled.
    CancelSibling { order_id: u64, group: u64 },
}

/// The trigger engine: holds conditional orders, evaluates ticks, resolves
/// OCO/OCA group semantics.
pub struct TriggerEngine {
    orders: Vec<ConditionalOrder>,
}

impl TriggerEngine {
    pub fn new() -> Self {
        TriggerEngine { orders: Vec::new() }
    }

    /// Register a held conditional order.
    pub fn hold(&mut self, order: ConditionalOrder) {
        self.orders.push(order);
    }

    /// Cancel a held order directly (client cancel). Cancels OCO siblings too.
    pub fn cancel(&mut self, order_id: u64) -> Vec<TriggerAction> {
        let mut actions = Vec::new();
        if let Some(pos) = self.orders.iter().position(|o| o.order_id == order_id) {
            let removed = self.orders.remove(pos);
            if let Some(group) = removed.oco_group {
                // OCO: cancel every remaining sibling in the group.
                let siblings: Vec<u64> = self
                    .orders
                    .iter()
                    .filter(|o| o.oco_group == Some(group))
                    .map(|o| o.order_id)
                    .collect();
                for sid in siblings {
                    let pos = self.orders.iter().position(|o| o.order_id == sid).unwrap();
                    self.orders.remove(pos);
                    actions.push(TriggerAction::CancelSibling {
                        order_id: sid,
                        group,
                    });
                }
            }
        }
        actions
    }

    /// Feed a market tick: evaluates every held order on that instrument.
    /// Fired orders are removed from the hold and their OCO siblings cancelled.
    pub fn on_tick(&mut self, tick: &MarketTick) -> Vec<TriggerAction> {
        let mut actions = Vec::new();
        let mut fired_groups: Vec<u64> = Vec::new();
        let mut fired_orders: Vec<u64> = Vec::new();
        let mut i = 0;
        while i < self.orders.len() {
            // Index-linked orders never evaluate against instrument quotes -
            // their reference arrives via `on_index`.
            if self.orders[i].trigger_ref == TriggerRef::Index {
                i += 1;
                continue;
            }
            let triggered = self.orders[i].evaluate(tick);
            match triggered {
                Some(action) => {
                    let removed = self.orders.remove(i);
                    if let Some(g) = removed.oco_group {
                        fired_groups.push(g);
                    }
                    fired_orders.push(removed.order_id);
                    actions.push(action);
                }
                None => i += 1,
            }
        }
        // OCO group resolution: cancel remaining siblings of fired groups.
        for g in fired_groups {
            actions.extend(self.cancel_group_siblings(g, &fired_orders));
        }
        actions
    }

    /// Feed an index publication: evaluates every index-linked held order
    /// (the `stop_index` condition type - the trigger watches the index
    /// engine's published level instead of the instrument's own quote).
    /// `index_id` scopes which index the publication belongs to: held
    /// orders carry it in `instrument_id` (the index's own id space).
    pub fn on_index(&mut self, index_publication: &IndexPublication) -> Vec<TriggerAction> {
        let mut actions = Vec::new();
        let mut fired_groups: Vec<u64> = Vec::new();
        let mut i = 0;
        while i < self.orders.len() {
            if self.orders[i].trigger_ref != TriggerRef::Index
                || self.orders[i].instrument_id != index_publication.index_id
            {
                i += 1;
                continue;
            }
            let fired = self.orders[i]
                .evaluate_reference(index_publication.value)
                .map(|a| matches!(a, TriggerAction::Release { .. }))
                .unwrap_or(false);
            if fired {
                let removed = self.orders.remove(i);
                if let Some(g) = removed.oco_group {
                    fired_groups.push(g);
                }
                actions.push(TriggerAction::Release {
                    order_id: removed.order_id,
                    is_buy: removed.is_buy,
                    limit_price: removed.limit_price,
                    triggered_at: index_publication.value,
                });
            } else {
                i += 1;
            }
        }
        // OCA group resolution (fired index-linked legs cancel siblings).
        let fired: Vec<u64> = actions
            .iter()
            .filter_map(|a| match a {
                TriggerAction::Release { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        for g in fired_groups {
            actions.extend(self.cancel_group_siblings(g, &fired));
        }
        actions
    }

    /// Cancel the remaining members of a fired group (OCO/OCA siblings).
    fn cancel_group_siblings(&mut self, group: u64, fired: &[u64]) -> Vec<TriggerAction> {
        let siblings: Vec<u64> = self
            .orders
            .iter()
            .filter(|o| o.oco_group == Some(group) && !fired.contains(&o.order_id))
            .map(|o| o.order_id)
            .collect();
        let mut actions = Vec::new();
        for sid in siblings {
            let pos = self.orders.iter().position(|o| o.order_id == sid).unwrap();
            self.orders.remove(pos);
            actions.push(TriggerAction::CancelSibling {
                order_id: sid,
                group,
            });
        }
        actions
    }

    /// Orders currently held (diagnostics/admin view).
    pub fn held(&self) -> &[ConditionalOrder] {
        &self.orders
    }
}

impl Default for TriggerEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(instr: u32, last: i128, bid: i128, ask: i128) -> MarketTick {
        MarketTick {
            instrument_id: instr,
            last: Some(last),
            bid: Some(bid),
            ask: Some(ask),
        }
    }

    #[test]
    fn buy_stop_fires_at_or_above_trigger() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            1,
            7,
            true,
            TriggerRef::Last,
            10_100,
            None,
        ));
        // Below: hold.
        assert!(e.on_tick(&tick(7, 10_000, 9_900, 10_050)).is_empty());
        // At: fire.
        let actions = e.on_tick(&tick(7, 10_100, 10_050, 10_150));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release {
            order_id,
            is_buy,
            limit_price,
            triggered_at,
        } = &actions[0]
        else {
            panic!()
        };
        assert_eq!((*order_id, *is_buy), (1, true));
        assert_eq!(*limit_price, None); // market on trigger
        assert_eq!(*triggered_at, 10_100);
        assert!(e.held().is_empty());
    }

    #[test]
    fn sell_stop_fires_at_or_below_trigger() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            2,
            7,
            false,
            TriggerRef::Last,
            9_900,
            Some(9_850),
        ));
        assert!(e.on_tick(&tick(7, 10_000, 9_900, 10_050)).is_empty());
        let actions = e.on_tick(&tick(7, 9_850, 9_800, 9_900));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release {
            order_id,
            is_buy,
            limit_price,
            ..
        } = &actions[0]
        else {
            panic!()
        };
        assert_eq!((*order_id, *is_buy), (2, false));
        assert_eq!(*limit_price, Some(9_850)); // stop-limit
    }

    #[test]
    fn stop_uses_bid_ask_reference() {
        let mut e = TriggerEngine::new();
        // Sell stop watching the bid.
        e.hold(ConditionalOrder::stop(
            3,
            7,
            false,
            TriggerRef::Bid,
            9_800,
            None,
        ));
        // Last is above but the bid is at the trigger: fire.
        let actions = e.on_tick(&tick(7, 10_000, 9_800, 10_050));
        assert_eq!(actions.len(), 1);
        // Buy stop watching the ask.
        let mut e2 = TriggerEngine::new();
        e2.hold(ConditionalOrder::stop(
            4,
            7,
            true,
            TriggerRef::Ask,
            10_200,
            None,
        ));
        assert!(e2.on_tick(&tick(7, 10_000, 9_900, 10_100)).is_empty());
        let actions = e2.on_tick(&tick(7, 10_000, 9_900, 10_200));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn missing_reference_does_not_fire() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            5,
            7,
            true,
            TriggerRef::Ask,
            10_200,
            None,
        ));
        // Tick with no ask: hold.
        let t = MarketTick {
            instrument_id: 7,
            last: Some(10_300),
            bid: Some(10_000),
            ask: None,
        };
        assert!(e.on_tick(&t).is_empty());
    }

    #[test]
    fn other_instruments_ignored() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            6,
            7,
            true,
            TriggerRef::Last,
            10_100,
            None,
        ));
        let t = MarketTick {
            instrument_id: 8,
            last: Some(11_000),
            bid: None,
            ask: None,
        };
        assert!(e.on_tick(&t).is_empty());
    }

    #[test]
    fn trailing_sell_stop_ratchets_up() {
        let mut e = TriggerEngine::new();
        // Sell trailing stop, offset 50 ticks, initial trigger 10_000.
        e.hold(ConditionalOrder::trailing(
            7,
            7,
            false,
            TriggerRef::Last,
            10_000,
            50,
            None,
        ));
        // Price rises: stop ratchets up to 10_200 − 50 = 10_150.
        let actions = e.on_tick(&tick(7, 10_200, 10_150, 10_250));
        assert!(actions.is_empty()); // not fired yet
        assert_eq!(e.held()[0].trigger_price, 10_150);
        // Price rises further: ratchet to 10_400 − 50 = 10_350.
        let actions = e.on_tick(&tick(7, 10_400, 10_350, 10_450));
        assert!(actions.is_empty());
        assert_eq!(e.held()[0].trigger_price, 10_350);
        // Price falls to the trailed stop: fire.
        let actions = e.on_tick(&tick(7, 10_300, 10_250, 10_350));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn trailing_buy_stop_ratchets_down() {
        let mut e = TriggerEngine::new();
        // Buy trailing stop, offset 50, initial trigger 9_000.
        e.hold(ConditionalOrder::trailing(
            8,
            7,
            true,
            TriggerRef::Last,
            9_000,
            50,
            None,
        ));
        // Price falls: stop ratchets down to 8_800 + 50 = 8_850.
        e.on_tick(&tick(7, 8_800, 8_750, 8_850));
        assert_eq!(e.held()[0].trigger_price, 8_850);
        // Further fall: 8_500 + 50 = 8_550.
        e.on_tick(&tick(7, 8_500, 8_450, 8_550));
        assert_eq!(e.held()[0].trigger_price, 8_550);
        // Rise to the stop: fire.
        let actions = e.on_tick(&tick(7, 8_550, 8_500, 8_600));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release { is_buy, .. } = &actions[0] else {
            panic!()
        };
        assert!(*is_buy);
    }

    #[test]
    fn trailing_never_loosens() {
        let mut e = TriggerEngine::new();
        // Sell trailing, offset 100, initial trigger 10_000.
        e.hold(ConditionalOrder::trailing(
            9,
            7,
            false,
            TriggerRef::Last,
            10_000,
            100,
            None,
        ));
        e.on_tick(&tick(7, 10_500, 10_450, 10_550));
        // Peak 10_500 − 100 = 10_400, above the initial 10_000 → ratchets up.
        assert_eq!(e.held()[0].trigger_price, 10_400);
        // Price dips but stays above the trailed stop: no fire, no loosen.
        e.on_tick(&tick(7, 10_450, 10_400, 10_500));
        assert_eq!(e.held()[0].trigger_price, 10_400);
        assert_eq!(e.held().len(), 1);
        // New high peak: ratchet follows up (10_700 − 100).
        e.on_tick(&tick(7, 10_700, 10_650, 10_750));
        assert_eq!(e.held()[0].trigger_price, 10_600);
        // And finally the drop through the stop fires it.
        let actions = e.on_tick(&tick(7, 10_300, 10_250, 10_350));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn oco_sibling_cancelled_on_fire() {
        let mut e = TriggerEngine::new();
        // Bracket: take-profit limit sell + stop-loss, one group.
        e.hold(
            ConditionalOrder::stop(10, 7, false, TriggerRef::Last, 10_500, Some(10_450))
                .in_group(1),
        );
        e.hold(
            ConditionalOrder::stop(11, 7, false, TriggerRef::Last, 9_500, Some(9_450)).in_group(1),
        );
        // Take-profit fires.
        let actions = e.on_tick(&tick(7, 10_500, 10_450, 10_550));
        assert_eq!(actions.len(), 2);
        assert!(matches!(
            actions[0],
            TriggerAction::Release { order_id: 10, .. }
        ));
        assert!(matches!(
            actions[1],
            TriggerAction::CancelSibling {
                order_id: 11,
                group: 1
            }
        ));
        assert!(e.held().is_empty());
    }

    #[test]
    fn oco_cancel_propagates_on_client_cancel() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(12, 7, false, TriggerRef::Last, 10_500, None).in_group(2));
        e.hold(ConditionalOrder::stop(13, 7, false, TriggerRef::Last, 9_500, None).in_group(2));
        // Client cancels one leg: the sibling is cancelled too.
        let actions = e.cancel(12);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            TriggerAction::CancelSibling {
                order_id: 13,
                group: 2
            }
        ));
        assert!(e.held().is_empty());
    }

    #[test]
    fn independent_orders_do_not_cross_cancel() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(14, 7, false, TriggerRef::Last, 10_500, None).in_group(3));
        e.hold(ConditionalOrder::stop(15, 7, false, TriggerRef::Last, 9_500, None).in_group(4));
        let actions = e.cancel(14);
        // Only group 3's order was cancelled; group 4 survives.
        assert!(actions.is_empty());
        assert_eq!(e.held().len(), 1);
        assert_eq!(e.held()[0].order_id, 15);
    }

    #[test]
    fn multiple_fires_in_one_tick() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            16,
            7,
            true,
            TriggerRef::Last,
            10_100,
            None,
        ));
        e.hold(ConditionalOrder::stop(
            17,
            7,
            true,
            TriggerRef::Last,
            10_050,
            None,
        ));
        e.hold(ConditionalOrder::stop(
            18,
            7,
            false,
            TriggerRef::Last,
            9_000,
            None,
        ));
        // Big up-tick: both buy stops fire, sell stop holds.
        let actions = e.on_tick(&tick(7, 10_200, 10_150, 10_250));
        assert_eq!(actions.len(), 2);
        assert_eq!(e.held().len(), 1);
        assert_eq!(e.held()[0].order_id, 18);
    }

    // ── stop_index (index-linked triggers - design order-types §2) ──────

    fn index_pub(index_id: u32, value: i128) -> IndexPublication {
        IndexPublication {
            index_id,
            value,
            ts_us: 1,
        }
    }

    #[test]
    fn stop_index_fires_on_index_publication() {
        let mut e = TriggerEngine::new();
        // Sell stop keyed to index 5 at level 20_000: fires at/below.
        e.hold(ConditionalOrder::stop(
            21,
            5,
            false,
            TriggerRef::Index,
            20_000,
            None,
        ));
        // Above the level: hold.
        assert!(e.on_index(&index_pub(5, 20_500)).is_empty());
        assert_eq!(e.held().len(), 1);
        // At the level: fire (at-or-below for a sell stop).
        let actions = e.on_index(&index_pub(5, 20_000));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release {
            order_id,
            is_buy,
            limit_price,
            triggered_at,
        } = &actions[0]
        else {
            panic!()
        };
        assert_eq!(*order_id, 21);
        assert!(!*is_buy);
        assert_eq!(*limit_price, None);
        assert_eq!(*triggered_at, 20_000);
        assert!(e.held().is_empty());
    }

    #[test]
    fn stop_index_buy_fires_at_or_above() {
        let mut e = TriggerEngine::new();
        // Buy stop keyed to index 9 at level 21_000: fires at/above.
        e.hold(ConditionalOrder::stop(
            22,
            9,
            true,
            TriggerRef::Index,
            21_000,
            Some(21_100),
        ));
        assert!(e.on_index(&index_pub(9, 20_950)).is_empty());
        let actions = e.on_index(&index_pub(9, 21_000));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release {
            order_id,
            limit_price,
            ..
        } = &actions[0]
        else {
            panic!()
        };
        assert_eq!(*order_id, 22);
        assert_eq!(*limit_price, Some(21_100)); // stop-limit on the index
        assert!(e.held().is_empty());
    }

    #[test]
    fn stop_index_ignores_other_indexes_and_ticks() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(
            23,
            5,
            true,
            TriggerRef::Index,
            20_000,
            None,
        ));
        // A different index's publication must not fire it.
        assert!(e.on_index(&index_pub(6, 30_000)).is_empty());
        assert_eq!(e.held().len(), 1);
        // Instrument quotes never fire index-linked orders either (the
        // quote path skips them).
        let actions = e.on_tick(&tick(5, 99_000, 98_500, 99_500));
        assert!(actions.is_empty());
        assert_eq!(e.held().len(), 1);
        // The right index at its level fires.
        let actions = e.on_index(&index_pub(5, 20_000));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn stop_index_trailing_ratchets_and_fires_on_the_way_back() {
        let mut e = TriggerEngine::new();
        // Sell trailing stop on an index: trails the highest publication
        // seen, offset 250.
        e.hold(ConditionalOrder::trailing(
            24,
            5,
            false,
            TriggerRef::Index,
            20_000,
            250,
            None,
        ));
        // Index rises to 20_800 → stop ratchets to 20_550.
        assert!(e.on_index(&index_pub(5, 20_800)).is_empty());
        assert_eq!(e.held()[0].trigger_price, 20_550);
        // Index falls through the trailed stop → fire.
        let actions = e.on_index(&index_pub(5, 20_500));
        assert_eq!(actions.len(), 1);
        let TriggerAction::Release { triggered_at, .. } = &actions[0] else {
            panic!()
        };
        assert_eq!(*triggered_at, 20_500);
    }

    // ── OCA (one-cancels-all - design order-types §2) ───────────────────

    #[test]
    fn oca_first_fire_releases_and_cancels_all_group_members() {
        let mut e = TriggerEngine::new();
        // Three unrelated orders sharing one OCA group (entered
        // independently - the OCA distinction: no constructed bracket).
        e.hold(ConditionalOrder::stop(31, 7, true, TriggerRef::Last, 10_100, None).in_group(50));
        e.hold(ConditionalOrder::stop(32, 7, false, TriggerRef::Last, 9_500, None).in_group(50));
        e.hold(ConditionalOrder::stop(33, 7, false, TriggerRef::Bid, 9_400, None).in_group(50));
        // The buy stop fires: legs 32 AND 33 both cancel (one-cancels-ALL).
        let actions = e.on_tick(&tick(7, 10_100, 10_050, 10_150));
        assert_eq!(actions.len(), 3);
        assert!(matches!(
            actions[0],
            TriggerAction::Release { order_id: 31, .. }
        ));
        let cancelled: Vec<u64> = actions[1..]
            .iter()
            .filter_map(|a| match a {
                TriggerAction::CancelSibling { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(cancelled, vec![32, 33]);
        assert!(e.held().is_empty());
    }

    #[test]
    fn oca_client_cancel_withdraws_the_whole_group() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(41, 7, true, TriggerRef::Last, 10_100, None).in_group(60));
        e.hold(ConditionalOrder::stop(42, 7, false, TriggerRef::Last, 9_500, None).in_group(60));
        e.hold(ConditionalOrder::stop(43, 7, false, TriggerRef::Last, 9_400, None).in_group(61));
        // Cancel ANY member of the OCA group → all members withdrawn.
        let actions = e.cancel(42);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            actions[0],
            TriggerAction::CancelSibling {
                order_id: 41,
                group: 60
            }
        ));
        // The other group's member was untouched.
        assert_eq!(e.held().len(), 1);
        assert_eq!(e.held()[0].order_id, 43);
    }

    #[test]
    fn oca_fired_member_cannot_double_fire() {
        let mut e = TriggerEngine::new();
        e.hold(ConditionalOrder::stop(51, 7, true, TriggerRef::Last, 10_100, None).in_group(70));
        e.hold(ConditionalOrder::stop(52, 7, true, TriggerRef::Last, 10_050, None).in_group(70));
        // One tick takes both above their stops: BOTH fire (independent
        // evaluations - OCA cancels only survivors, never a fired leg).
        let actions = e.on_tick(&tick(7, 10_200, 10_150, 10_250));
        let releases: Vec<u64> = actions
            .iter()
            .filter_map(|a| match a {
                TriggerAction::Release { order_id, .. } => Some(*order_id),
                _ => None,
            })
            .collect();
        assert_eq!(releases.len(), 2);
        assert!(releases.contains(&51) && releases.contains(&52));
        assert!(e.held().is_empty());
    }
}
