// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS peg-order state machine.
//!
//! Implements the OMS design §4.4 state machine (see the service
//! `DESIGN.md` §4 for the transition set):
//!
//! ```text
//! [*] --> OnHold: AddPegOrder (ack PegReceived → PegBooked(limit))
//! OnHold --> Executing: quote actionable (submit slice)
//! OnHold --> Cancelled: cancel while idle
//! Executing --> Modifying: quote changed (CFO to ME)
//! Modifying --> Executing: ME modified ok
//! Executing --> Pending: not actionable (ME cancel)
//! Pending --> Executing: re-evaluate actionable
//! Pending --> OnHold: re-evaluate not actionable
//! Executing --> Cancelled: ME killed (peg stop / drift)
//! Modifying --> Cancelled: ME killed
//! Cancelled --> [*]
//! ```
//!
//! The machine drives the working slice against the ME via the caller
//! (`PegController::decide` returns the ME actions to perform). Pricing
//! delegates to `pegs::compute_peg_price`.
//!
//! Design: `design/sentin8-exchange8-oms-technical-design.md` §4.4.

use exchange8::fixed::Price;

use crate::pegs::{compute_peg_price, PegKind, PegOutcome, PegSide, Quote};

/// Peg lifecycle state (design §4.1 `status` field).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PegState {
    /// No actionable quote yet (or one-sided); acked at limit, not on ME.
    OnHold,
    /// A slice is working on the ME at the computed peg price.
    Executing,
    /// A cancel-replace is in flight to the ME.
    Modifying,
    /// The working slice was pulled (not actionable); awaiting re-evaluation.
    Pending,
    /// Terminal.
    Cancelled,
}

/// Why the peg entered `Cancelled`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelCause {
    /// Client cancel request.
    Client,
    /// ME killed the slice (peg stop / drift).
    MeKilled,
}

/// ME action the controller requests from the caller.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PegAction {
    /// Submit a new working slice at `price` (qty = peg qty).
    Submit { price: Price },
    /// Cancel the working slice and replace at `price` (CFO).
    Replace { price: Price },
    /// Cancel the working slice (gone non-actionable).
    Withdraw,
    /// No ME action this evaluation.
    None,
}

/// One managed peg order.
pub struct PegOrder {
    pub peg_id: u64,
    pub kind: PegKind,
    pub side: PegSide,
    pub quote: Quote,
    pub offset: i128,
    /// Client limit — the peg never prices through it; reaching it
    /// converts the peg to a plain limit order.
    pub limit: Price,
    /// Terminal conversion flag: once true the peg is a limit order.
    pub converted_to_limit: bool,
    state: PegState,
    /// Working slice price (None while OnHold/Pending).
    working: Option<Price>,
}

impl PegOrder {
    pub fn new(peg_id: u64, kind: PegKind, side: PegSide, offset: i128, limit: Price) -> Self {
        PegOrder {
            peg_id,
            kind,
            side,
            quote: Quote {
                best_bid: None,
                best_ask: None,
            },
            offset,
            limit,
            converted_to_limit: false,
            state: PegState::OnHold,
            working: None,
        }
    }

    pub fn state(&self) -> PegState {
        self.state
    }

    /// The peg's status key — the admin/RFW projection of the state
    /// (`on_hold|executing|modifying|pending|cancelled`, the lowercase
    /// keys the `pegs` admin page and the peg events carry).
    pub fn status_key(&self) -> &'static str {
        match self.state {
            PegState::OnHold => "on_hold",
            PegState::Executing => "executing",
            PegState::Modifying => "modifying",
            PegState::Pending => "pending",
            PegState::Cancelled => "cancelled",
        }
    }

    pub fn working(&self) -> Option<Price> {
        self.working
    }

    /// Feed a new quote snapshot and decide the next ME action
    /// (the transition table in design §4.4).
    pub fn on_quote(&mut self, quote: Quote) -> PegAction {
        if self.state == PegState::Cancelled {
            return PegAction::None;
        }
        self.quote = quote;
        if self.converted_to_limit {
            // A converted peg is a plain limit order — no repricing.
            return PegAction::None;
        }
        match compute_peg_price(
            self.kind,
            self.side,
            quote,
            self.offset,
            self.limit,
            self.working,
        ) {
            Ok(PegOutcome::Actionable(price)) => match self.state {
                PegState::OnHold | PegState::Pending => {
                    // quote actionable (re-)submit the slice.
                    self.state = PegState::Executing;
                    self.working = Some(price);
                    PegAction::Submit { price }
                }
                PegState::Executing => {
                    if self.working != Some(price) {
                        // quote changed → CFO to ME.
                        self.state = PegState::Modifying;
                        PegAction::Replace { price }
                    } else {
                        PegAction::None
                    }
                }
                PegState::Modifying => PegAction::None, // already in flight
                PegState::Cancelled => PegAction::None,
            },
            Ok(PegOutcome::Hold) => match self.state {
                PegState::Executing => {
                    // not actionable → pull the slice, await re-evaluation.
                    self.state = PegState::Pending;
                    self.working = None;
                    PegAction::Withdraw
                }
                PegState::Pending => {
                    // re-evaluate: still not actionable → back to OnHold.
                    self.state = PegState::OnHold;
                    PegAction::None
                }
                _ => PegAction::None,
            },
            Ok(PegOutcome::ConvertToLimit(_)) => {
                // Reached the limit: becomes a plain limit order at `limit`.
                // If nothing is working, submit at limit; if a slice is
                // working, replace it at limit. Terminal for repricing.
                self.converted_to_limit = true;
                match self.state {
                    PegState::OnHold | PegState::Pending => {
                        self.state = PegState::Executing;
                        self.working = Some(self.limit);
                        PegAction::Submit { price: self.limit }
                    }
                    _ => {
                        self.working = Some(self.limit);
                        PegAction::Replace { price: self.limit }
                    }
                }
            }
            // Off-tick midpoint: hold (no unsafe price); the next quote
            // re-evaluates.
            Err(_) => match self.state {
                PegState::Executing => {
                    self.state = PegState::Pending;
                    self.working = None;
                    PegAction::Withdraw
                }
                _ => PegAction::None,
            },
        }
    }

    /// ME ack: the CFO landed (Modifying → Executing at the new price).
    pub fn on_modify_ok(&mut self, new_price: Price) {
        if self.state == PegState::Modifying {
            self.state = PegState::Executing;
            self.working = Some(new_price);
        }
    }

    /// ME killed the working slice (peg stop / drift).
    pub fn on_me_killed(&mut self) -> CancelCause {
        self.state = PegState::Cancelled;
        self.working = None;
        CancelCause::MeKilled
    }

    /// Client cancel — valid in any non-terminal state.
    pub fn cancel(&mut self) -> Result<CancelCause, &'static str> {
        if self.state == PegState::Cancelled {
            return Err("already cancelled");
        }
        self.state = PegState::Cancelled;
        self.working = None;
        Ok(CancelCause::Client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange8::fixed::Price;

    fn px(raw: i128) -> Price {
        Price::equity(raw)
    }

    fn quote(bid: i128, ask: i128) -> Quote {
        Quote {
            best_bid: Some(px(bid)),
            best_ask: Some(px(ask)),
        }
    }

    #[test]
    fn starts_on_hold_and_ignores_empty_quote() {
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 0, px(10_000));
        assert_eq!(p.state(), PegState::OnHold);
        // One-sided quote → stays OnHold, no action.
        let action = p.on_quote(Quote {
            best_bid: Some(px(9_900)),
            best_ask: None,
        });
        assert_eq!(action, PegAction::None);
        assert_eq!(p.state(), PegState::OnHold);
    }

    #[test]
    fn actionable_quote_submits_slice() {
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 100, px(10_500));
        // Primary bid = bid + offset = 9_900 + 100 = 10_000 ≤ limit.
        match p.on_quote(quote(9_900, 10_100)) {
            PegAction::Submit { price } => assert_eq!(price.raw, 10_000),
            other => panic!("expected submit, got {other:?}"),
        }
        assert_eq!(p.state(), PegState::Executing);
        assert_eq!(p.working().map(|w| w.raw), Some(10_000));
    }

    #[test]
    fn quote_change_goes_through_modifying() {
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 100, px(10_500));
        p.on_quote(quote(9_900, 10_100)); // submit at 10_000
                                          // Bid improves to 9_950 → new peg price 10_050 → CFO.
        match p.on_quote(quote(9_950, 10_150)) {
            PegAction::Replace { price } => assert_eq!(price.raw, 10_050),
            other => panic!("expected replace, got {other:?}"),
        }
        assert_eq!(p.state(), PegState::Modifying);
        // ME ack lands at the new price.
        p.on_modify_ok(px(10_050));
        assert_eq!(p.state(), PegState::Executing);
        assert_eq!(p.working().map(|w| w.raw), Some(10_050));
        // Same quote again → no action.
        assert_eq!(p.on_quote(quote(9_950, 10_150)), PegAction::None);
    }

    #[test]
    fn one_sided_quote_pulls_then_reholds() {
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 100, px(10_500));
        p.on_quote(quote(9_900, 10_100)); // Executing at 10_000
                                          // Quote goes one-sided → withdraw, Pending.
        assert_eq!(
            p.on_quote(Quote {
                best_bid: Some(px(9_900)),
                best_ask: None
            }),
            PegAction::Withdraw
        );
        assert_eq!(p.state(), PegState::Pending);
        // Still one-sided → Pending → OnHold.
        assert_eq!(
            p.on_quote(Quote {
                best_bid: Some(px(9_900)),
                best_ask: None
            }),
            PegAction::None
        );
        assert_eq!(p.state(), PegState::OnHold);
        // Quote returns → submit again.
        match p.on_quote(quote(9_900, 10_100)) {
            PegAction::Submit { price } => assert_eq!(price.raw, 10_000),
            other => panic!("expected submit, got {other:?}"),
        }
        assert_eq!(p.state(), PegState::Executing);
    }

    #[test]
    fn limit_reached_converts_to_limit() {
        // Primary bid + offset would cross the limit: convert.
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 100, px(10_000));
        // bid 10_050 + 100 = 10_150 > limit 10_000 → convert at limit.
        match p.on_quote(quote(10_050, 10_200)) {
            PegAction::Submit { price } => assert_eq!(price.raw, 10_000),
            other => panic!("expected submit at limit, got {other:?}"),
        }
        assert!(p.converted_to_limit);
        // Further quotes never reprice a converted peg.
        assert_eq!(p.on_quote(quote(9_000, 9_100)), PegAction::None);
        assert_eq!(p.working().map(|w| w.raw), Some(10_000));
    }

    #[test]
    fn me_killed_and_client_cancel_terminal() {
        let mut p = PegOrder::new(1, PegKind::Primary, PegSide::Bid, 100, px(10_500));
        p.on_quote(quote(9_900, 10_100));
        assert_eq!(p.on_me_killed(), CancelCause::MeKilled);
        assert_eq!(p.state(), PegState::Cancelled);
        assert!(p.on_quote(quote(9_900, 10_100)) == PegAction::None);
        assert!(p.cancel().is_err());

        let mut p2 = PegOrder::new(2, PegKind::Primary, PegSide::Bid, 100, px(10_500));
        assert_eq!(p2.cancel().unwrap(), CancelCause::Client);
        assert_eq!(p2.state(), PegState::Cancelled);
    }

    #[test]
    fn market_peg_ratchet_never_prices_worse() {
        // Market bid = ask − offset; working price ratchets (never lower).
        let mut p = PegOrder::new(1, PegKind::Market, PegSide::Bid, 100, px(10_500));
        match p.on_quote(quote(9_900, 10_100)) {
            PegAction::Submit { price } => assert_eq!(price.raw, 10_000), // ask−100
            other => panic!("expected submit, got {other:?}"),
        }
        // Ask falls to 10_000 → computed 9_900 < working 10_000 → ratchet
        // keeps 10_000 (no replace).
        assert_eq!(p.on_quote(quote(9_800, 10_000)), PegAction::None);
        assert_eq!(p.working().map(|w| w.raw), Some(10_000));
        // Ask rises → 10_200−100 = 10_100 > working → CFO up.
        match p.on_quote(quote(9_900, 10_200)) {
            PegAction::Replace { price } => assert_eq!(price.raw, 10_100),
            other => panic!("expected replace, got {other:?}"),
        }
    }
}
