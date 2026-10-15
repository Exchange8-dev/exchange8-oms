// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS typed event envelopes (DBX producer path).
//!
//! Every peg/conditional/algo mutation writes an outbox row in the SAME
//! transaction as the domain change with a **payload-complete** envelope
//! (event-first rule — consumers react from the payload alone):
//! `event_type`, `env_id`, subject, correlation/idempotency ids, and the
//! full domain snapshot. The shell's outbox relay (its boot step 8b)
//! pushes the rows to Sentin8:Bus `POST /v1/produce` and marks them
//! relayed; the audit pusher (8c) drains the journal rows.
//!
//! Stream family: `exchange8` — the Bus stream key is
//! `bus:{env_id}:exchange8`; the family slug is validated by
//! `sentin8::bus::outbox::validate_stream_family` (colons are structurally
//! impossible in a family; event NAMES carry them), matching the sibling
//! convention (refdata leaves).
//! Event names follow the vertical vocabulary
//! `exchange8:oms:{resource}:{operation}` (DESIGN.md §4 table).
//!
//! Design: `services/oms/DESIGN.md` §4 (event vocabulary + flow), §10.

use serde_json::{json, Value};
use uuid::Uuid;

/// Stream family for all OMS-emitted events (bare vertical slug — the Bus
/// `XADD bus:{env}:exchange8` key; colons are structurally impossible per
/// `sentin8::bus::outbox::validate_stream_family`).
pub const STREAM_FAMILY: &str = "exchange8";

// ── Event vocabulary (DESIGN.md §4 table — colon segments only) ─────────

/// `exchange8:oms:peg:received` — a peg order was accepted (client ack).
pub const EV_PEG_RECEIVED: &str = "exchange8:oms:peg:received";
/// `exchange8:oms:peg:booked` — the peg confirmed at its limit (OnHold)
/// or its first slice started working (Executing).
pub const EV_PEG_BOOKED: &str = "exchange8:oms:peg:booked";
/// `exchange8:oms:peg:repriced` — the working slice repriced (or the peg
/// converted to a plain limit order; `converted` flag distinguishes).
pub const EV_PEG_REPRICED: &str = "exchange8:oms:peg:repriced";
/// `exchange8:oms:peg:killed` — the peg left supervision (ME kill /
/// client cancel / withdraw with no re-hold).
pub const EV_PEG_KILLED: &str = "exchange8:oms:peg:killed";
/// `exchange8:oms:peg:executed` — a fill landed on the working slice
/// (fill-forward with the aggregate fields).
pub const EV_PEG_EXECUTED: &str = "exchange8:oms:peg:executed";
/// `exchange8:oms:peg:held` — the peg pulled its slice (quote went
/// one-sided / crossed): Executing → Pending/OnHold.
pub const EV_PEG_HELD: &str = "exchange8:oms:peg:held";
/// `exchange8:oms:conditional:armed` — a conditional/stop order registered
/// (held; never rests in the ME book).
pub const EV_CONDITIONAL_ARMED: &str = "exchange8:oms:conditional:armed";
/// `exchange8:oms:conditional:released` — the trigger fired; the order
/// released to Exchange8:ME (market or stop-limit terms).
pub const EV_CONDITIONAL_RELEASED: &str = "exchange8:oms:conditional:released";
/// `exchange8:oms:conditional:sibling-cancelled` — an OCO/OCA sibling
/// cancelled (group semantics resolved in the engine).
pub const EV_CONDITIONAL_SIBLING_CANCELLED: &str = "exchange8:oms:conditional:sibling-cancelled";
/// `exchange8:oms:algo:started` — an algo parent began running
/// (`oms.algo_order.state = running`).
pub const EV_ALGO_STARTED: &str = "exchange8:oms:algo:started";
/// `exchange8:oms:algo:paused` — the parent paused (slice plan halted).
pub const EV_ALGO_PAUSED: &str = "exchange8:oms:algo:paused";
/// `exchange8:oms:algo:resumed` — the parent resumed.
pub const EV_ALGO_RESUMED: &str = "exchange8:oms:algo:resumed";
/// `exchange8:oms:algo:child-issued` — one child slice emitted to ME
/// (TWAP/VWAP interval or POV participation step).
pub const EV_ALGO_CHILD_ISSUED: &str = "exchange8:oms:algo:child-issued";
/// `exchange8:oms:algo:completed` — the parent consumed its full quantity.
pub const EV_ALGO_COMPLETED: &str = "exchange8:oms:algo:completed";
/// `exchange8:oms:algo:cancelled` — the parent was withdrawn before
/// completion.
pub const EV_ALGO_CANCELLED: &str = "exchange8:oms:algo:cancelled";

/// The identity carried inside every envelope (handler-owned, never derived
/// from DB or headers — event-first: consumers re-key their caches without
/// callbacks).
pub struct EventIdentity {
    /// Tenant environment of the event.
    pub env_id: Uuid,
    /// Acting subject (JWT `sub`; the supervisor's M2M client id for the
    /// quote-driven path).
    pub subject_id: String,
    /// Correlation id (from `X-Sentin8-Correlation-Id` when present —
    /// otherwise generated).
    pub correlation_id: String,
    /// Idempotency key: consumers dedupe on this.
    pub idempotency_key: String,
}

/// Write the outbox row for an envelope in the CALLER's transaction
/// (same tx as the domain change — outbox-first). The relay (shell boot
/// step 8b) pushes it to Bus post-commit; failure of the write aborts the
/// caller's tx via `?` at the call site. `publisher_subject` carries the
/// request's verified `sub` so consumers can attribute the event.
///
/// # Errors
/// Propagates the SQL error (the validator's rejection of the constant
/// family text is structurally impossible — covered by the registry test).
pub async fn emit_outbox(
    tx: &mut sqlx::PgConnection,
    identity: &EventIdentity,
    envelope: &Value,
) -> Result<Uuid, sqlx::Error> {
    let ev = envelope["event_type"].as_str().unwrap_or_default();
    match sentin8::bus::outbox::insert_outbox_row_with_publisher(
        &mut *tx,
        identity.env_id,
        STREAM_FAMILY,
        ev,
        envelope,
        None,
        Some(&identity.subject_id),
    )
    .await
    {
        Ok(id) => Ok(id),
        Err(sentin8::bus::outbox::OutboxError::Db(e)) => Err(e),
        Err(e) => Err(sqlx::Error::Configuration(Box::new(e))),
    }
}

/// Which peg mutation produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PegOp {
    /// Peg accepted (client ack at limit).
    Received,
    /// Peg confirmed / first slice working.
    Booked,
    /// Working slice repriced (or converted to limit).
    Repriced,
    /// Peg left supervision (ME kill / client cancel).
    Killed,
    /// Fill landed on the working slice.
    Executed,
    /// Slice pulled (non-actionable quote).
    Held,
}

/// Envelope for a peg lifecycle mutation: `snapshot` carries the peg
/// identity + lifecycle fields (env, peg id, instrument, state, working
/// price, limit, conversion flag, trigger cause) — Gateway (client
/// confirmations) and Clearing (audit/clearing mirror) react from the
/// payload alone.
pub fn peg_event(op: PegOp, snapshot: Value, identity: &EventIdentity) -> Value {
    let event_type = match op {
        PegOp::Received => EV_PEG_RECEIVED,
        PegOp::Booked => EV_PEG_BOOKED,
        PegOp::Repriced => EV_PEG_REPRICED,
        PegOp::Killed => EV_PEG_KILLED,
        PegOp::Executed => EV_PEG_EXECUTED,
        PegOp::Held => EV_PEG_HELD,
    };
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": snapshot,
    })
}

/// Which conditional mutation produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionalOp {
    /// Registered (armed = true, held, not in the ME book).
    Armed,
    /// Trigger fired; order released to ME.
    Released,
    /// OCO/OCA sibling cancelled (group resolution).
    SiblingCancelled,
}

/// Envelope for a conditional-order mutation: `snapshot` carries the
/// conditional identity (order id, condition type, trigger terms, trigger
/// reference at fire time, group id when grouped).
pub fn conditional_event(op: ConditionalOp, snapshot: Value, identity: &EventIdentity) -> Value {
    let event_type = match op {
        ConditionalOp::Armed => EV_CONDITIONAL_ARMED,
        ConditionalOp::Released => EV_CONDITIONAL_RELEASED,
        ConditionalOp::SiblingCancelled => EV_CONDITIONAL_SIBLING_CANCELLED,
    };
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": snapshot,
    })
}

/// Which algo mutation produced the event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlgoOp {
    /// Parent started (`state = running`).
    Started,
    /// Parent paused (`running → paused`; durable binding).
    Paused,
    /// Parent resumed (`paused → running`).
    Resumed,
    /// Child slice emitted.
    ChildIssued,
    /// Parent completed (`state = complete`).
    Completed,
    /// Parent cancelled (`state = cancelled`).
    Cancelled,
}

/// Envelope for an algo-order mutation: `snapshot` carries the parent
/// identity + the transition (parent id, algo type, child slice when
/// applicable, remaining qty, durable state before/after).
pub fn algo_event(op: AlgoOp, snapshot: Value, identity: &EventIdentity) -> Value {
    let event_type = match op {
        AlgoOp::Started => EV_ALGO_STARTED,
        AlgoOp::Paused => EV_ALGO_PAUSED,
        AlgoOp::Resumed => EV_ALGO_RESUMED,
        AlgoOp::ChildIssued => EV_ALGO_CHILD_ISSUED,
        AlgoOp::Completed => EV_ALGO_COMPLETED,
        AlgoOp::Cancelled => EV_ALGO_CANCELLED,
    };
    json!({
        "event_type": event_type,
        "env_id": identity.env_id,
        "subject_id": identity.subject_id,
        "correlation_id": identity.correlation_id,
        "idempotency_key": identity.idempotency_key,
        "snapshot": snapshot,
    })
}

// ── Audit journal (architecture-principles §6.8) ─────────────────────────

/// Audit `action` words accepted by [`journal_mutation`] (maps one-to-one to
/// `sentin8_audit::AuditAction`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOp {
    /// Resource created.
    Create,
    /// Resource modified.
    Update,
    /// Resource deleted.
    Delete,
    /// Lifecycle executed (trigger fire, slice re-arm, schedule run).
    Execute,
}

/// Build the audit envelope for one mutation and write the
/// `oms.audit_journal` row on the CALLER's transaction (same tx as the
/// domain change; the shell's pusher — boot step 8c — drains it to
/// Sentin8:Sys `POST /v1/audit` with the `oms-audit` M2M workload).
/// Quote-driven re-evaluations are NOT journalized (pricing state, not a
/// durable mutation) — armed/released/paused/resumed/cancelled are.
pub async fn journal_mutation(
    tx: &mut sqlx::PgConnection,
    identity: &EventIdentity,
    op: AuditOp,
    resource_type: &str,
    resource_id: &str,
    message: &str,
    before: Option<Value>,
    after: Option<Value>,
) -> Result<(), sqlx::Error> {
    use sentin8::audit::{AuditAction, AuditEvent, AuditSeverity};
    let action = match op {
        AuditOp::Create => AuditAction::Create,
        AuditOp::Update => AuditAction::Update,
        AuditOp::Delete => AuditAction::Delete,
        AuditOp::Execute => AuditAction::Execute,
    };
    let ev = AuditEvent::builder()
        .service("exchange8:oms")
        .action(action)
        .resource(resource_type, resource_id)
        .actor(&identity.subject_id, "machine")
        .tenant(Uuid::nil(), identity.env_id)
        .severity(AuditSeverity::Info)
        .correlation_id(&identity.correlation_id)
        .message(message);
    let ev = match before {
        Some(b) => ev.before(b),
        None => ev,
    };
    let ev = match after {
        Some(aft) => ev.after(aft),
        None => ev,
    };
    sentin8::audit::journal_insert(&mut *tx, &ev.build()).await
}

/// Registry test coverage (DESIGN.md §9): every declared event name uses
/// the platform event format (`vertical:service:resource:operation`, all
/// colons — no dots) and the stream family validates.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_names_follow_the_colon_format() {
        const NAMES: &[&str] = &[
            EV_PEG_RECEIVED,
            EV_PEG_BOOKED,
            EV_PEG_REPRICED,
            EV_PEG_KILLED,
            EV_PEG_EXECUTED,
            EV_PEG_HELD,
            EV_CONDITIONAL_ARMED,
            EV_CONDITIONAL_RELEASED,
            EV_CONDITIONAL_SIBLING_CANCELLED,
            EV_ALGO_STARTED,
            EV_ALGO_PAUSED,
            EV_ALGO_RESUMED,
            EV_ALGO_CHILD_ISSUED,
            EV_ALGO_COMPLETED,
            EV_ALGO_CANCELLED,
        ];
        for n in NAMES {
            // Format contract: `vertical:service:resource:operation` —
            // all-colon segments, no dots. Hyphenated segments (the
            // child-issued / sibling-cancelled vocabulary) are the
            // platform's scope convention.
            let segs: Vec<&str> = n.split(':').collect();
            assert_eq!(segs.len(), 4, "{n}: must have 4 segments");
            assert_eq!(segs[0], "exchange8", "{n}: wrong vertical");
            assert_eq!(segs[1], "oms", "{n}: wrong service");
            assert!(segs.iter().all(|s| !s.is_empty()), "{n}");
            assert!(n.find('.').is_none(), "{n}: dots are not event format");
        }
    }

    #[test]
    fn stream_family_validator_safe() {
        assert!(
            sentin8::bus::outbox::validate_stream_family(STREAM_FAMILY).is_ok(),
            "colons are structurally impossible in a stream family"
        );
        assert!(sentin8::bus::outbox::validate_stream_family("exchange8:oms").is_err());
    }

    #[test]
    fn envelopes_carry_the_complete_identity_block() {
        let identity = EventIdentity {
            env_id: uuid::Uuid::nil(),
            subject_id: "supervisor".into(),
            correlation_id: "corr-1".into(),
            idempotency_key: "idem-1".into(),
        };
        for envelope in [
            peg_event(PegOp::Received, json!({ "peg_id": 1 }), &identity),
            peg_event(PegOp::Booked, json!({ "state": "on_hold" }), &identity),
            peg_event(PegOp::Repriced, json!({ "price": 10100 }), &identity),
            peg_event(PegOp::Killed, json!({ "cause": "me_killed" }), &identity),
            peg_event(PegOp::Executed, json!({ "qty": 100 }), &identity),
            peg_event(PegOp::Held, json!({}), &identity),
            conditional_event(ConditionalOp::Armed, json!({ "armed": true }), &identity),
            conditional_event(ConditionalOp::Released, json!({ "ref": 9900 }), &identity),
            conditional_event(ConditionalOp::SiblingCancelled, json!({}), &identity),
            algo_event(AlgoOp::Started, json!({ "state": "running" }), &identity),
            algo_event(AlgoOp::Paused, json!({ "state": "paused" }), &identity),
            algo_event(AlgoOp::Resumed, json!({ "state": "running" }), &identity),
            algo_event(AlgoOp::ChildIssued, json!({ "slice": 1 }), &identity),
            algo_event(AlgoOp::Completed, json!({ "state": "complete" }), &identity),
            algo_event(
                AlgoOp::Cancelled,
                json!({ "state": "cancelled" }),
                &identity,
            ),
        ] {
            assert!(envelope["event_type"].as_str().is_some());
            assert_eq!(envelope["env_id"], identity.env_id.to_string());
            assert_eq!(envelope["subject_id"], "supervisor");
            assert_eq!(envelope["correlation_id"], "corr-1");
            assert_eq!(envelope["idempotency_key"], "idem-1");
            assert!(envelope["snapshot"].is_object());
        }
        // The three families map 1:1 to their vocabulary constants.
        assert_eq!(
            peg_event(PegOp::Repriced, json!({}), &identity)["event_type"],
            EV_PEG_REPRICED
        );
        assert_eq!(
            conditional_event(ConditionalOp::Released, json!({}), &identity)["event_type"],
            EV_CONDITIONAL_RELEASED
        );
        assert_eq!(
            algo_event(AlgoOp::ChildIssued, json!({}), &identity)["event_type"],
            EV_ALGO_CHILD_ISSUED
        );
    }

    #[test]
    fn algo_transition_ops_state_the_target_state() {
        // The consumed family: the durable algo state binding (DESIGN.md
        // §3) maps every transition to exactly one op.
        let id = EventIdentity {
            env_id: Uuid::nil(),
            subject_id: "s".into(),
            correlation_id: "c".into(),
            idempotency_key: "k".into(),
        };
        assert_eq!(
            algo_event(AlgoOp::Paused, json!({}), &id)["event_type"],
            "exchange8:oms:algo:paused"
        );
        assert_eq!(
            algo_event(AlgoOp::Resumed, json!({}), &id)["event_type"],
            "exchange8:oms:algo:resumed"
        );
        assert_eq!(
            algo_event(AlgoOp::Completed, json!({}), &id)["event_type"],
            "exchange8:oms:algo:completed"
        );
    }
}
