// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS peg-order engine core.
//!
//! Peg lifecycle: on-hold → executing → (re)pricing on quote updates →
//! cancel/fill. Price formulas preserve the the platform spec §peg
//! semantics exactly (design OMS §4.3):
//!
//! - `Primary`   : bid + offset / ask − offset (join own side)
//! - `Market`    : ask − offset (bid side) / bid + offset (ask side) — join
//!                 the *opposite* side, with a monotonic ratchet (never
//!                 reprice worse than the working price)
//! - `Midpoint`  : (bid+ask)/2 + offset — only when bid ≠ ask and both live;
//!                 validated against the **doubled tick** grid
//! - `Darkpool`  : midpoint formula, always Hidden
//!
//! No quote / one-sided quote → the peg stays `OnHold` (ack at limit).
//! A visible peg reaching its limit **converts to a plain limit order**.
//!
//! Design: `design/sentin8-exchange8-oms-technical-design.md` §4.3–4.4.

use exchange8::fixed::{on_doubled_equity_tick, Price};

/// Peg type (mirror of the contracts PegType; kept local for formula
/// clarity — the OMS owns the pricing domain).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PegKind {
    /// Join own side: bid + offset / ask − offset.
    Primary,
    /// Join opposite side with a monotonic ratchet.
    Market,
    /// Midpoint of the quote; doubled-tick validated.
    Midpoint,
    /// Midpoint, always hidden (dark pool).
    Darkpool,
}

/// Quote snapshot for one instrument (own-book BBO or consolidated EBBO).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
}

impl Quote {
    fn both_live(&self) -> bool {
        self.best_bid.is_some() && self.best_ask.is_some()
    }

    fn crossed_or_locked(&self) -> bool {
        match (self.best_bid, self.best_ask) {
            (Some(b), Some(a)) => b.raw >= a.raw,
            _ => false,
        }
    }
}

/// Side the peg order rests on (ME side semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PegSide {
    Bid,
    Ask,
}

/// Why a computed price was rejected.
///
/// `NoQuote`/`CrossedQuote`/`ViolatesLimit` are constructed by the state
/// machine's transition diagnostics (see DESIGN.md §4.4); the pure pricing
/// core maps them to `Hold`/`ConvertToLimit` outcomes directly.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PegError {
    /// No quote or one-sided quote — the peg stays OnHold.
    NoQuote,
    /// Crossed/locked quote — unsafe to price from.
    CrossedQuote,
    /// Midpoint result violates the doubled-tick grid.
    OffDoubledTick,
    /// Computed price crosses the peg's limit.
    ViolatesLimit,
}

/// Outcome of a peg evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PegOutcome {
    /// Price computed and actionable; submit/modify the slice at this price.
    Actionable(Price),
    /// Not actionable — hold (or pull the working slice); ack at limit.
    Hold,
    /// The peg has reached its limit — it becomes a plain limit order at
    /// `limit` and stops repricing (conversion rule).
    ConvertToLimit(Price),
}

/// Compute the peg price for one evaluation (design OMS §4.3 table).
///
/// `offset` is in price ticks (raw units at the instrument's scale) —
/// `PegDifference` (FIX 211) sign semantics: positive offset moves the price
/// *away* from the inside for Primary (bid+offset = more aggressive buy).
pub fn compute_peg_price(
    kind: PegKind,
    side: PegSide,
    quote: Quote,
    offset: i128,
    limit: Price,
    // Current working price (for the Market-peg ratchet); `None` on first
    // submission.
    working: Option<Price>,
) -> Result<PegOutcome, PegError> {
    if !quote.both_live() {
        return Ok(PegOutcome::Hold);
    }
    if quote.crossed_or_locked() {
        return Ok(PegOutcome::Hold);
    }
    let bid = quote.best_bid.unwrap();
    let ask = quote.best_ask.unwrap();

    let computed = match (kind, side) {
        (PegKind::Primary, PegSide::Bid) => bid.raw + offset,
        (PegKind::Primary, PegSide::Ask) => ask.raw - offset,
        (PegKind::Market, PegSide::Bid) => ask.raw - offset,
        (PegKind::Market, PegSide::Ask) => bid.raw + offset,
        (PegKind::Midpoint, _) | (PegKind::Darkpool, _) => {
            // Only when bid ≠ ask (no midpoint inside a locked quote).
            if bid.raw == ask.raw {
                return Ok(PegOutcome::Hold);
            }
            // (bid+ask)/2 + offset — integer division truncates down, the
            // engine used the same floor math on raw tick units.
            let mid = (bid.raw + ask.raw) / 2 + offset;
            // Doubled-tick validation: the midpoint of a doubled grid is a
            // half-tick; rejects it rather than rounding.
            let probe = Price {
                raw: mid,
                scale: bid.scale,
            };
            if !on_doubled_equity_tick(probe) {
                return Err(PegError::OffDoubledTick);
            }
            mid
        }
    };

    let price = Price {
        raw: computed,
        scale: bid.scale,
    };

    // Market-peg monotonic ratchet: never reprice worse than the working
    // price (bid: lower is worse; ask: higher is worse).
    let price = match (kind, working) {
        (PegKind::Market, Some(w)) => match side {
            PegSide::Bid => {
                if price.raw < w.raw {
                    w
                } else {
                    price
                }
            }
            PegSide::Ask => {
                if price.raw > w.raw {
                    w
                } else {
                    price
                }
            }
        },
        _ => price,
    };

    // Limit relation: bid ≤ limit; ask ≥ limit.
    let within_limit = match side {
        PegSide::Bid => price.raw <= limit.raw,
        PegSide::Ask => price.raw >= limit.raw,
    };
    if !within_limit {
        // At/through the limit → convert to a plain limit order .
        return Ok(PegOutcome::ConvertToLimit(limit));
    }

    Ok(PegOutcome::Actionable(price))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(bid: i128, ask: i128) -> Quote {
        Quote {
            best_bid: Some(Price::equity(bid)),
            best_ask: Some(Price::equity(ask)),
        }
    }

    #[test]
    fn primary_bid_joins_own_side() {
        // bid 10.00, offset 0.01 (100 ticks) → 10.01; limit 10.05.
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Bid,
            q(10_000, 10_100),
            100,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_100)));
    }

    #[test]
    fn primary_ask_minus_offset() {
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Ask,
            q(10_000, 10_100),
            100,
            Price::equity(9_900),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_000)));
    }

    #[test]
    fn market_peg_joins_opposite() {
        // Market bid = ask − offset.
        let out = compute_peg_price(
            PegKind::Market,
            PegSide::Bid,
            q(10_000, 10_100),
            0,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_100)));
    }

    #[test]
    fn market_peg_ratchet_never_worse() {
        // Working at 10.10; new quote would compute 10.00 — keep 10.10.
        let out = compute_peg_price(
            PegKind::Market,
            PegSide::Bid,
            q(9_900, 10_000),
            0,
            Price::equity(10_500),
            Some(Price::equity(10_100)),
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_100)));
    }

    #[test]
    fn market_peg_ratchet_allows_better() {
        // Working at 10.10; new quote computes 10.20 → take it.
        let out = compute_peg_price(
            PegKind::Market,
            PegSide::Bid,
            q(10_100, 10_200),
            0,
            Price::equity(10_500),
            Some(Price::equity(10_100)),
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_200)));
    }

    #[test]
    fn midpoint_requires_spread() {
        // Locked quote (bid == ask) → hold.
        let out = compute_peg_price(
            PegKind::Midpoint,
            PegSide::Bid,
            q(10_000, 10_000),
            0,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Hold);
    }

    #[test]
    fn midpoint_on_doubled_tick_grid() {
        // bid 9.98 / ask 10.02 → mid 10.00 = 100_000 raw — on the doubled
        // (200-tick = 0.02) grid.
        let out = compute_peg_price(
            PegKind::Midpoint,
            PegSide::Bid,
            q(9_800, 10_200),
            0,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_000)));
    }

    #[test]
    fn midpoint_off_grid_rejected() {
        // bid 10.00 / ask 10.02 → mid 10.01 = 100_100 — half of the doubled
        // tick (0.01 vs required 0.02 grid) → compute error.
        let out = compute_peg_price(
            PegKind::Midpoint,
            PegSide::Bid,
            q(10_000, 10_200),
            0,
            Price::equity(10_500),
            None,
        );
        assert_eq!(out, Err(PegError::OffDoubledTick));
    }

    #[test]
    fn darkpool_same_as_midpoint() {
        // bid 9.98 / ask 10.02 → mid 10.00 (on the doubled grid).
        let out = compute_peg_price(
            PegKind::Darkpool,
            PegSide::Ask,
            q(9_800, 10_200),
            0,
            Price::equity(9_900),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Actionable(Price::equity(10_000)));
    }

    #[test]
    fn no_quote_holds() {
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Bid,
            Quote {
                best_bid: None,
                best_ask: Some(Price::equity(10_100)),
            },
            0,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Hold);
    }

    #[test]
    fn crossed_quote_holds() {
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Bid,
            q(10_200, 10_100),
            0,
            Price::equity(10_500),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::Hold);
    }

    #[test]
    fn limit_violation_converts_to_limit() {
        // Bid peg computed 10.20 but limit 10.10 → convert.
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Bid,
            q(10_200, 10_300),
            0,
            Price::equity(10_100),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::ConvertToLimit(Price::equity(10_100)));
    }

    #[test]
    fn ask_limit_floor_enforced() {
        // Ask peg computed 9.95 but limit 10.00 → convert.
        let out = compute_peg_price(
            PegKind::Primary,
            PegSide::Ask,
            q(9_900, 10_000),
            500,
            Price::equity(10_000),
            None,
        )
        .unwrap();
        assert_eq!(out, PegOutcome::ConvertToLimit(Price::equity(10_000)));
    }
}
