# BUSINESS.md: Ex8:OMS — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Ex8:OMS **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Let institutions trade without showing full size or intent: pegged orders
  track the market automatically (own side, opposite side, or the midpoint,
  with a dark hidden variant), and iceberg orders reveal only a small
  display slice while the true size stays hidden in reserve.
- Protect clients from chasing a moving market by hand: pegs reprice on
  every relevant quote change within a hard client limit, and never trade
  at a price worse than the one they are already working at.
- Hold conditional orders (stops, stop-limits, trailing stops, take-profit /
  stop-loss brackets) out of the public book until their trigger fires, so
  market participants cannot see or front-run the intent.
- Automate execution styles (TWAP / VWAP / POV slices, market-on-open /
  market-on-close auction participation) so large parent orders trade per
  plan without a human babysitting each child.

## 2. Business Scope

### What the service provides

| Capability | Description |
|---|---|
| Pegged orders (primary) | Working price joins the peg's own side of the book (bid + offset / ask − offset), repriced as the quote moves |
| Pegged orders (market) | Working price joins the opposite side with a monotonic ratchet — once working, the price only improves |
| Pegged orders (midpoint) | Works at the quote midpoint (spread required); validated against the venue's doubled-tick grid |
| Dark pegged orders | Midpoint-priced, never visibly displayed — hidden execution, not a separate dark pool |
| Iceberg execution | Display-slice reserve accounting: fill the visible slice, re-arm the next from the hidden reserve |
| Stop / stop-limit orders | Held and released to the matching engine when the reference price (last / bid / ask) breaches the trigger |
| Trailing stops | Trigger trails the best reference seen by a fixed offset, only ever tightening |
| OCO / OCA groups | One-cancels-other / one-cancels-all groups: the first leg to fire executes and every survivor cancels; client-cancel of any member withdraws the whole group |
| Index-linked stops | `stop_index` triggers watch a published index level (not the instrument's own quote) — trailing variants ratchet on the index the same way |
| Complex (multi-leg) orders | Atomic all-or-none evaluation across legs — the strategy trades in full or not at all |
| Execution algorithms | TWAP (uniform slices), VWAP (volume-profile-weighted slices), POV (percentage of realized volume) |
| Auction participation | MOC / MOO / LOC / LOO orders queued for the auction uncross (never book-resting), with imbalance contribution |
| Quote-driven re-evaluation | Per-instrument BBO/EBBO quote cache drives exactly the pegs on the changed instrument |
| Child-order identity & idempotency | Scene-leased child order ids and client-key dedup — duplicate submissions return the original ack |
| Lifecycle mirroring | Peg confirmations to the gateway and durable event mirrors for clearing/audit consumers (outbox family `exchange8` — one row per mutation, audit-journaled) |
| Algo parent control | Operators pause/resume a running parent from the admin console; the durable `oms.algo_order.state` carries the plan across restarts |
| Admin surfaces | Shield policy administration plus declared RFW admin pages for live pegs, held conditionals, and the algo parents (with pause/resume actions) |
| Supervision gauges | The peg/conditional/algo counts + child-slice counter emit to the platform stat surface |

### What the service does NOT own (business boundary)

| Concern | Owning service |
|---|---|
| Client connectivity, FIX/sessions, order state shown to the client | Ex8:Gateway |
| The matching books, fills, and execution prices | Ex8:ME |
| Pre-trade risk checks (position, credit, fat-finger limits) | Ex8:Risk |
| Smart order routing decisions | Ex8:SOR |
| Market-data book assembly and feed publication | Ex8:MD / Ex8:VFeeds |
| Reference data: instruments, tick rules, calendars | Ex8:RefData |
| Trade persistence, clearing, netting | Ex8:Clearing |
| Settlement and margin | Ex8:Settlement / Ex8:Margin |
| Surveillance & regulatory reporting | Ex8:Surveillance / Ex8:Regulatory |
| Identity, tokens, authorization decisions engine | Sentin8:Auth / Shield (declared sets) |
| Event durability & fan-out infrastructure | Sentin8:Bus |
| Audit archival | Sentin8:Sys |

## 3. Business Capabilities

### Pegged orders

| Capability | Business rule |
|---|---|
| Primary peg | Buying works at best-bid + offset; selling works at best-ask − offset — the client states the offset from the side it would join |
| Market peg | Buying works near the ask, selling near the bid (joins the opposite side); once working, the price only improves — a market that would reprice worse is ignored |
| Midpoint peg | Works at the midpoint of a live two-sided quote; a locked market (bid = ask) or a one-sided book holds the order rather than inventing a price |
| Doubled-tick integrity | A midpoint that does not land on the venue's doubled price grid is held, never rounded to a near tick |
| Dark peg | Prices like midpoint but the order is always hidden — dark execution is a display choice, not a separate matching venue |
| Peg limit is a hard ceiling/floor | Computed prices never cross the client limit; reaching the limit converts the peg into an ordinary limit order and repricing stops for good |
| No-quote behavior | Without a two-sided live quote the peg acknowledges at the limit price and waits — it does not send an unprotected price to the book |
| Continuous repricing | Every quote change re-evaluates the peg; a change on a working peg issues a cancel-replace at the computed price |

### Icebergs

| Capability | Business rule |
|---|---|
| Display floor | Only `max_floor` is ever visible and matchable; the hidden reserve is revealed only as previous slices trade out |
| Slice re-arm | When the displayed slice is exhausted and quantity remains, the next slice arms at min(max_floor, remaining) and re-enters the book at the queue tail |
| Sub-order identity | Each re-arm increments the slice counter, giving every displayed instance its own identity at the matching engine |
| Partial fills | A partial fill stays within the displayed slice; repricing/re-arm only when the slice fully trades out |
| Slice cancel | Cancelling the displayed slice pulls it while the reserve stays; the iceberg ends by fill-out or kill, not by a single cancel |

### Stop / trigger orders

| Capability | Business rule |
|---|---|
| Held intent | Stops, stop-limits, trailing stops, and conditional/bracket legs never rest in the public book — they activate only when the trigger fires |
| Trigger references | The trigger may watch last trade, best bid, or best ask; buy stops fire at-or-above the stop, sell stops at-or-below |
| Release terms | On fire, the order releases as a market order or with its stop-limit price; the client learns the actual reference at fire time |
| Trailing stops | The stop trails the best reference seen by a fixed offset; it only tightens — a market move that would loosen the protection is ignored |
| OCO / OCA groups | The first leg to trigger releases; remaining legs cancel automatically. Canceling one leg (client request) withdraws the whole bracket |
| Idempotent triggers | Re-triggering an already-fired conditional is an explicit "already fired" response, not a silent duplicate |
| Double-fire safety | Several held orders on one instrument can fire from one market move; each gets its own release, and group siblings close without cross-cancelling unrelated orders |

### Execution algorithms & auctions

| Capability | Business rule |
|---|---|
| TWAP | The parent quantity splits into uniform slices over the chosen horizon; any indivisible remainder is front-loaded one per interval and the plan always conserves total quantity |
| VWAP | Slices follow the expected per-interval volume profile — a 3:1 profile puts roughly three-quarters of the remaining parent into the first interval |
| POV | Participation is a percentage of realized volume per interval, capped by the parent's remaining quantity; the plan runs open-ended until the parent is exhausted |
| Complex orders | A multi-leg strategy executes only when every leg can fill its full ratio simultaneously at its limit — a single short leg means nothing trades |
| Auction participation | MOC/MOO (market) and LOC/LOO (limit) orders queue for the auction; their quantities feed the published indicative imbalance before the uncross |

## 4. Business Events (Outbound)

Outbound events (the `exchange8:{service}:{resource}:{operation}`
vocabulary; outbox family `exchange8` — one row per mutation in the SAME
transaction, relayed idempotently by the shell, audit-journaled):

| Event family | When it fires |
|---|---|
| `exchange8:oms:peg:received` / `booked` / `repriced` / `held` / `killed` / `executed` | Peg acknowledge, first working slice, each reprice or conversion, slice pull, kill by ME or client, fills |
| `exchange8:oms:conditional:armed` / `released` / `sibling-cancelled` | A stop registers, its trigger fires releasing the order, or an OCO/OCA sibling closes |
| `exchange8:oms:algo:started` / `paused` / `resumed` / `completed` / `cancelled` | The algo parent's durable lifecycle transitions (running ⇄ paused; terminal on completion/cancel) |
| `exchange8:oms:algo:child-issued` | Each child slice issued by a TWAP/VWAP/POV parent |

Consumers: Ex8:Gateway (client confirmations with the client-facing order
id restored), Ex8:Clearing (audit/clearing mirror). Inbound events
consumed: quote updates from Ex8:MD / Ex8:VFeeds (BBO / consolidated EBBO),
lifecycle events from Ex8:ME (booked / killed / modified / executed /
rejected / repriced), Shield policy-set updates.

## 5. Business Rules & Invariants

1. A peg never trades outside its client limit: the limit is a hard
   ceiling (buy) / floor (sell); touching it converts the peg to an
   ordinary limit order and repricing stops permanently.
2. A market peg already working in the market never reprices to a worse
   price; improvements are taken immediately.
3. Midpoint and dark prices are always valid on the venue price grid; an
   unusable midpoint holds the order instead of approximating.
4. One-sided, crossed, or locked markets hold pegs (and pull any working
   slice) rather than sending unprotected prices.
5. Conditional orders never rest in the public book before their trigger —
   no participant can observe the intent before release.
6. Trailing stops only tighten; the protection level ratchets with the
   favorable extreme and never relaxes.
7. In an OCO group, exactly one leg can fire; after that the group is
   terminal and re-firing has no effect; cancelling any leg closes the
   whole group.
8. A complex order trades atomically: every leg in full, or nothing.
9. Iceberg reserve is never displayed or matchable beyond the display
   floor; every re-arm moves quantity from reserve to displayed and
   re-enters the market at the back of the queue.
10. Duplicate order submissions with the same idempotency token return the
    original acknowledgment; duplicate client keys return the originally
    assigned child order id — no double leasing.
11. Every lifecycle decision produces a durable event record before
    delivery; delivery retries until the event plane accepts it (audit is
    delayed, never lost).
12. No environment can see or influence another environment's pegs,
    conditionals, or algo parents (row-level security is forced on all
    durable tables; working state is per-environment memory).
13. Authorization is fail-closed: without a matching Shield permit,
    requests are rejected; shadow policies never block; tenant forbids
    always override the platform mirror.
14. A matching-engine or quote outage never corrupts peg state: quotes stop
    repricing (hold last), release-on-trigger pauses safely, and restart
    reconciles against live engine states before resuming.

## 6. Target-User Roles & Use Cases

| Persona | Role | What they do with Ex8:OMS |
|---|---|---|
| Institutional trader | Large-order executor | Runs iceberg and pegged orders to accumulate or exit size without moving the visible market |
| Algorithmic desk | Execution-algo user | Runs TWAP/VWAP/POV parents whose child slices the OMS schedules and monitors |
| Retail / professional trader | Conditional-order user | Places stops, stop-limits, trailing stops, and take-profit/stop-loss brackets that activate automatically |
| Options trader | Spread trader | Submits multi-leg strategies traded atomically as one order |
| Market maker | Quoting member | Uses dark/midpoint pegs for passive quoting that tracks the market without leaking intent |
| Buy-side operations | Order supervisor | Watches live peg/conditional state (state, working price, limit, trigger status) in the admin console |
| Exchange operations | Market operator | Sees held vs working pegs on halted instruments and reconciles restarts |
| Venue administrator | Shield admin | Manages the OMS policy sets (enforce/shadow), relations, and attributes via the admin API |
| Clearing & compliance | Consumer | Consumes the durable lifecycle mirror for reconciliation and audit |

## 7. KPIs / Success Criteria

- **Reprice fidelity**: zero prices off the venue grid, zero trades through
  a client limit, zero adverse market-peg reprices — every reprice provably
  derives from the quote at that moment.
- **Intent protection**: no conditional order rests in the public book;
  trigger evaluation never reveals held terms to participants.
- **Slice-plan conservation**: every algo plan and iceberg executes exactly
  its parent quantity — no child leakage, no over- or under-execution, and
  duplicate idempotent submissions never create extra orders.
- **Responsiveness**: quote-driven repricing keeps each instrument's pegs
  current within the bounded re-evaluation set (one changed quote costs
  only that instrument's pegs, capped and alerted).
- **Availability**: rolling upgrades complete without lost working pegs or
  stale registry entries (drain + reconciliation + replay); event delivery
  resumes automatically after Bus/Sys outages with no loss.
- **Isolation**: zero cross-environment information flow (RLS matrix green
  in every suite run).
- **Auditability**: every reprice, conversion, trigger fire, sibling
  cancel, and slice re-arm is durable and reconstructable end-to-end.

## 8. Dependencies & Reliance

- **Ex8:Gateway** — the only peg-management entry point; forwards
  client orders, idempotency tokens, and receives confirmations with the
  client-facing id restored.
- **Ex8:MD / Ex8:VFeeds** — the quote inputs (BBO/EBBO); pegs stop
  repricing when quotes stall.
- **Ex8:ME** — the matching destination for working slices and released
  stops; its lifecycle events drive slice re-arm and kill handling.
- **Ex8:Risk** — pre-trade authorization happens upstream; OMS only
  re-validates tick compliance of computed prices.
- **Ex8:Clearing** — consumes the lifecycle mirror as the durable
  reconciliation set.
- **Sentin8:Auth** — all token issuance/verification; M2M workloads
  (`exchange8:oms`/`oms` streams, `oms-audit`, `exchange8-oms-tls`) and
  cert-bound defaults.
- **Sentin8:Policy (Shield)** — the only authorization gate; policy-set
  cache shared between admin and runtime paths.
- **Sentin8:Bus / Sentin8:Sys** — durable event delivery (outbox relay)
  and audit ingest; both degrade to delayed delivery, never loss.
- **Sentin8:DB (PostgreSQL 18, db `exchange8`, schema `oms`)** —
  conditional/complex/algo skeletons, outbox, audit journal; never touched
  without a tenant context.
- **Platform shell (`sentin8::shell`)** — registration, pool, health,
  drain, allocator; and `sentin8-tls` for in-process post-quantum TLS.

## 9. Acceptance Criteria

1. A primary-peg buy works at best-bid + offset and reprices with every
   quote change while staying at or under its client limit; touching the
   limit converts it to a plain limit order and stops repricing.
2. A market-peg order joins the opposite side and, once working, never
   moves to a worse price even as the quote deteriorates; improvements
   propagate immediately.
3. A midpoint peg holds on a one-sided, crossed, or locked quote and holds
   (rather than rounding) when the computed midpoint lands off the
   doubled-tick grid.
4. A dark peg never displays: it prices like midpoint and rests hidden.
5. An iceberg reveals at most `max_floor`; each exhausted slice re-arms
   from reserve with an incremented slice counter and re-enters at the
   queue tail; a final fill ends the order without re-arming.
6. A stop releases only when its reference price (last/bid/ask) at-or-
   crosses the trigger, in the correct direction per side; a stop-limit
   releases with its limit carried.
7. A trailing stop trails the best reference seen by the offset, only
   tightens, and fires when the reference returns through the trailed level.
8. OCO: the first leg to fire executes and every sibling cancels; client
   cancelling one leg closes the whole group; re-firing a terminal group is
   a no-op and the API reports "already fired" (409).
9. A complex order with one short leg executes nothing; all legs fill or
   none do.
10. TWAP/VWAP/POV plans consume exactly their parent quantity (uniform /
    profile-weighted / volume-participation respectively) and stop at zero.
11. MOC/MOO/LOC/LOO orders contribute to the published indicative
    imbalance and drain only at the uncross.
12. Duplicate submissions (same idempotency token or client key) return the
    original ack/child id — no duplicate orders, no double lease.
13. Every durable table is RLS-forced with provable env isolation (no
    context → zero rows; cross-env → zero rows; contextless insert →
    rejected).
14. Authorization is enforced solely by Shield's seeded scope mirror:
    fail-closed without sets, shadow never blocks, tenant forbids win,
    M2M parity holds.
15. Events persist durably before delivery: outbox rows written in the
    domain transaction, relayed idempotently to Bus, retried on outage, and
    DLQ-replayable on permanent failure; audit is delayed, never lost.
16. Restart reconciles working pegs against live ME states (unknown →
    re-acked as killed), re-arms held conditionals from durable rows, and
    re-evaluates from current quotes before resuming.
17. Rolling instance upgrades complete with graceful drain and no stale
    registry entries or lost reprice decisions.

## 10. Open Business Questions

- Business-side confirmation of the event payload contract for the peg
  lifecycle mirror (the implemented payloads are payload-complete — the
  envelope carries the full supervision snapshot; consumer sign-off on the
  per-field projection for Gateway/Clearing remains the open confirmation).
- Index-linked triggers (`stop_index` implemented against index
  publications): which indexes qualify and the publication latency
  tolerance are an exchange-policy decision.
- Participation-cap and peg-count limits per environment (the
  `INSTRUMENT_PEG_CAP` default of 5 000 live pegs per instrument):
  tenant-visible configurability is pending with the admin surface.
- Auction participation rules for MOC/MOO quantities (imbalance
  publication frequency during the call, and how queued quantities surface
  to the indicative feed) are shared with the matching engine's auction
  workflow pending its business sign-off.
- Fee/charge treatment of dark (midpoint) executions: the venue carries
  the midpoint charge flag downstream; the retail/institutional fee
  schedule per order type remains a business rules decision with the
  fee/billing owner.

---

**Design attribution:** exchange8:oms and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
