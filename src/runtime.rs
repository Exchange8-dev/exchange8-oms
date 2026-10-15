// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Ex8:OMS runtime wiring — the instrument-sharded evaluation plane + admin
//! handlers (DESIGN.md §7 task graph).
//!
//! Wires the pure cores (`pegs`, `peg_machine`, `triggers`, `order_types`)
//! onto the runtime:
//!
//! - **Quote consumer task** — one Bus consumer over the vertical's stream
//!   family: every consolidated BBO change (`exchange8:md:bbo:updated`,
//!   payload `exchange8::contracts::events::BboUpdated`) routes into the
//!   instrument's shard queue by `instrument_id % N` (the same hash shape
//!   the ME shards use). The producer side (MD/VFeeds publish sites) is
//!   their own wiring pass — the consumer runs against the declared family
//!   and idles (fail-open) until the producer publishes (consumers may run
//!   ahead of producers; the seam is listed in DESIGN.md §10).
//! - **Shard evaluator tasks** — one task per instrument shard owning a
//!   `TriggerEngine` + the quote cache + the peg list; single-writer (no
//!   locks on the evaluation path).
//! - **Algo scheduler task** — the interval pump advancing running
//!   `oms.algo_order` parents: paused/complete/cancelled parents idle, and
//!   every transition is bound durably (the `running|paused|complete|
//!   cancelled` CHECK vocabulary) with the transition event + audit journal
//!   row in one transaction (DESIGN.md §3).
//! - **Admin handlers** — the declared RFW pages (`pegs`, `conditionals`)
//!   + the `algos` data/pause/resume surface, Shield-gated per the refdata
//!   `handlers::admin_ui_page` pattern.
//!
//! Working pegs and trigger holds are memory-resident (DESIGN.md §3 —
//! rebuildable from Gateway replay / Clearing mirror); the durable
//! conditional/algo rows are DB-bound and authoritative.
//!
//! Design: `services/oms/DESIGN.md` §2 (runtime architecture), §7 (concurrency).

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use sqlx::Acquire;
use uuid::Uuid;

use sentin8::shell::AppState;

use crate::events::{AlgoOp, AuditOp, EventIdentity};
use crate::order_types::QuoteCache;
use crate::pegs::Quote;
use crate::triggers::ConditionalOrder;

/// Default shard count — the ME's hash shape (design §7: the same
/// `instrument_id % N` sharding as the matching engine).
pub const DEFAULT_SHARDS: usize = 64;

/// The instrument shard assignment: `instrument_id % N_SHARDS` (ME parity).
pub fn shard_of(instrument_id: u32, shards: usize) -> usize {
    (instrument_id as usize) % shards.max(1)
}

/// The per-instrument peg-count cap (design §9 sizing; the count is visible
/// on the `pegs` admin page).
pub const INSTRUMENT_PEG_CAP: usize = 5_000;

// ---------------------------------------------------------------------------
// Shard plane (single writer per instrument shard — DESIGN.md §7)
// ---------------------------------------------------------------------------

/// One shard's runtime state: the trigger engine + the quote cache + the
/// peg list. Owned by exactly one task; the mutex guards only cross-task
/// handoff at the admin snapshot (never a hot-path evaluation).
#[derive(Default)]
pub struct ShardRuntime {
    /// Held conditional orders (`stop_*`/`trailing_stop`/`oco`/`oca`).
    pub triggers: crate::triggers::TriggerEngine,
    /// Per-instrument quote snapshots + the change-filtered dirty set.
    pub quotes: QuoteCache,
    /// Live pegs on this shard's instruments (peg id → machine).
    pub pegs: HashMap<u64, crate::peg_machine::PegOrder>,
}

/// The process-wide shard runtimes (set once at boot; the admin handlers
/// read through the shared map, the consumer routes via the sender map).
static SHARD_RUNTIMES: std::sync::OnceLock<
    Arc<HashMap<usize, Arc<tokio::sync::Mutex<ShardRuntime>>>>,
> = std::sync::OnceLock::new();
static SHARD_SENDERS: std::sync::OnceLock<
    Arc<HashMap<usize, exchange8::wire::BoundedSender<ShardInput>>>,
> = std::sync::OnceLock::new();

/// Snapshot the shard runtimes (`None` pre-boot — the admin handlers then
/// serve empty tables).
pub fn shards() -> Option<&'static Arc<HashMap<usize, Arc<tokio::sync::Mutex<ShardRuntime>>>>> {
    SHARD_RUNTIMES.get()
}

/// One instrument's instrument-id projection for the peg machine (the peg
/// stores the quote only; the shard key rides the registration input).
pub fn instrument_of(peg: &crate::peg_machine::PegOrder) -> Option<u32> {
    instrument_index(peg.peg_id)
}

/// The peg → instrument projection: the peg registration carries the
/// instrument in its id (the supervisor's registry map `peg_id →
/// instrument_id` mirrors the Gateway id restore — DESIGN.md §4.1 record).
fn instrument_index(peg_id: u64) -> Option<u32> {
    INSTRUMENT_INDEX.lock().unwrap().get(&peg_id).copied()
}

/// The peg registries — process-wide sharded-lock maps (the registration
/// writer and the evaluator/every shard reader share them; lock hold is
/// O(1) map access, never an evaluation).
static INSTRUMENT_INDEX: std::sync::LazyLock<std::sync::Mutex<HashMap<u64, u32>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));
static PEG_ENV: std::sync::LazyLock<std::sync::Mutex<HashMap<u64, Uuid>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// One inbound evaluation message (the union the shard queues carry).
#[derive(Debug, Clone)]
pub enum ShardInput {
    /// A consolidated BBO change (`exchange8:md:bbo:updated` payload).
    Quote {
        /// Instrument the quote belongs to.
        instrument_id: u32,
        /// The bid/ask snapshot (raw ticks; `None` = empty side).
        quote: Quote,
    },
    /// A conditional registration (armed; never rests in the ME book).
    ArmConditional(ConditionalOrder),
}

/// Apply one input to the shard's owned state (the pure-core call
/// sequence; every peg ME-action and trigger fire projects into the egress
/// plane's outbox rows — the lifecycle mirror families, DESIGN.md §4).
async fn evaluate(rt: &tokio::sync::Mutex<ShardRuntime>, input: ShardInput) {
    // Collect the egress work OUTSIDE the shard lock (the projections are
    // pure staging calls).
    let mut staged: Vec<(u32, Vec<crate::triggers::TriggerAction>)> = Vec::new();
    {
        let mut shard = rt.lock().await;
        match input {
            ShardInput::Quote {
                instrument_id,
                quote,
            } => {
                shard.quotes.on_quote(
                    instrument_id,
                    crate::order_types::QuoteSnapshot {
                        bid: quote.best_bid,
                        ask: quote.best_ask,
                        ebbo: false,
                        ts_us: 0,
                    },
                );
                // The dirty set drives the re-evaluation pass (one pass
                // per quote — the change-filtered peg re-price + the
                // instrument tick's trigger evaluation).
                for dirty in shard.quotes.take_dirty() {
                    let Some(snapshot) = shard.quotes.quote(dirty) else {
                        continue;
                    };
                    let q = Quote {
                        best_bid: snapshot.bid,
                        best_ask: snapshot.ask,
                    };
                    for (peg_id, _instr) in pegs_on(&shard.pegs, dirty) {
                        if let Some(peg) = shard.pegs.get_mut(&peg_id) {
                            let action = peg.on_quote(q);
                            project_peg_action(peg_id, peg, &action);
                        }
                    }
                    // Trigger evaluation on the same quote (bid/ask
                    // references; last-referenced orders wait for a trade
                    // print tick).
                    let tick = crate::triggers::MarketTick {
                        instrument_id: dirty,
                        last: None,
                        bid: snapshot.bid.map(|p| p.raw),
                        ask: snapshot.ask.map(|p| p.raw),
                    };
                    let actions = shard.triggers.on_tick(&tick);
                    if !actions.is_empty() {
                        staged.push((dirty, actions));
                    }
                }
            }
            ShardInput::ArmConditional(order) => {
                stage_armed_mirror(&order);
                shard.triggers.hold(order);
            }
        }
    }
    for (instrument_id, actions) in staged {
        project_trigger_actions(instrument_id, actions);
    }
}

/// Stage the armed mirror for one conditional registration (the
/// conditional family's `armed` event rides the egress stage).
fn stage_armed_mirror(order: &ConditionalOrder) {
    let identity = EventIdentity {
        env_id: sentin8::config::SYSTEM_ENV_ID,
        subject_id: "exchange8:oms".into(),
        correlation_id: Uuid::new_v4().to_string(),
        idempotency_key: format!("conditional-armed:{}", order.order_id),
    };
    stage_event(StagedEvent {
        env_id: identity.env_id,
        envelope: crate::events::conditional_event(
            crate::events::ConditionalOp::Armed,
            serde_json::json!({
                "order_id": order.order_id,
                "instrument_id": order.instrument_id,
                "trigger_ref": format!("{:?}", order.trigger_ref),
                "trigger_price": order.trigger_price,
                "oco_group": order.oco_group,
            }),
            &identity,
        ),
        subject_id: identity.subject_id.clone(),
        correlation_id: identity.correlation_id.clone(),
        idempotency_key: identity.idempotency_key.clone(),
    });
}

/// Project one trigger-engine action set into the egress stage (the
/// conditional family mirrors — release + sibling-cancel pairs).
fn project_trigger_actions(instrument_id: u32, actions: Vec<crate::triggers::TriggerAction>) {
    for action in actions {
        let identity = EventIdentity {
            env_id: sentin8::config::SYSTEM_ENV_ID,
            subject_id: "exchange8:oms".into(),
            correlation_id: Uuid::new_v4().to_string(),
            idempotency_key: match &action {
                crate::triggers::TriggerAction::Release { order_id, .. } => {
                    format!("conditional-released:{order_id}")
                }
                crate::triggers::TriggerAction::CancelSibling { order_id, .. } => {
                    format!("conditional-sibling:{order_id}")
                }
            },
        };
        let (op, snapshot) = match &action {
            crate::triggers::TriggerAction::Release {
                order_id,
                is_buy,
                limit_price,
                triggered_at,
            } => (
                crate::events::ConditionalOp::Released,
                serde_json::json!({
                    "order_id": order_id,
                    "instrument_id": instrument_id,
                    "is_buy": is_buy,
                    "limit_price": limit_price,
                    "triggered_at": triggered_at,
                }),
            ),
            crate::triggers::TriggerAction::CancelSibling { order_id, group } => (
                crate::events::ConditionalOp::SiblingCancelled,
                serde_json::json!({
                    "order_id": order_id,
                    "group": group,
                }),
            ),
        };
        stage_event(StagedEvent {
            env_id: identity.env_id,
            envelope: crate::events::conditional_event(op, snapshot, &identity),
            subject_id: identity.subject_id.clone(),
            correlation_id: identity.correlation_id.clone(),
            idempotency_key: identity.idempotency_key.clone(),
        });
    }
}

/// The pegs of one instrument (the shard's peg map is small; the scan is
/// the change-driven re-evaluation set).
fn pegs_on(
    pegs: &HashMap<u64, crate::peg_machine::PegOrder>,
    instrument_id: u32,
) -> Vec<(u64, u32)> {
    pegs.keys()
        .filter_map(|id| {
            instrument_index(*id)
                .filter(|i| *i == instrument_id)
                .map(|i| (*id, i))
        })
        .collect()
}

/// The in-process peg-action registry: the egress projector's stage for the
/// supervision plane's ME actions + the event mirrors (the writer task
/// drains them into `oms.outbox` — batched 1000 events / 5 ms, the ME
/// writer budget; DESIGN.md §4 egress plane).
static EGRESS_STAGE: std::sync::OnceLock<(
    tokio::sync::mpsc::Sender<StagedEvent>,
    tokio::sync::Mutex<tokio::sync::mpsc::Receiver<StagedEvent>>,
)> = std::sync::OnceLock::new();

/// One staged egress event: the outbox envelope + identity (the writer
/// task formats the row; the evaluator never does DB work).
#[derive(Debug)]
pub struct StagedEvent {
    /// Tenant environment.
    pub env_id: Uuid,
    /// The payload-complete envelope (peg/conditional/algo vocabulary).
    pub envelope: serde_json::Value,
    /// The acting subject (the supervisor identity or the admin caller).
    pub subject_id: String,
    /// Correlation id.
    pub correlation_id: String,
    /// Idempotency key (consumers dedupe).
    pub idempotency_key: String,
}

/// Stage one event for the outbox writer (non-blocking; a full stage drops
/// the event with a warn — the durable writer applies the batch budget and
/// a saturated stage is the same backpressure the ME egress channel has).
pub fn stage_event(ev: StagedEvent) {
    if let Some((tx, _)) = EGRESS_STAGE.get() {
        if tx.try_send(ev).is_err() {
            tracing::warn!("oms egress stage saturated — event dropped");
        }
    }
}

/// Open the egress stage channel (once — `spawn_tasks` calls it first).
fn open_egress_stage() -> &'static (
    tokio::sync::mpsc::Sender<StagedEvent>,
    tokio::sync::Mutex<tokio::sync::mpsc::Receiver<StagedEvent>>,
) {
    EGRESS_STAGE.get_or_init(|| {
        let (tx, rx) = tokio::sync::mpsc::channel::<StagedEvent>(32_000);
        (tx, tokio::sync::Mutex::new(rx))
    })
}

/// Writer budget — the ME egress writer's batching (1000 events / 5 ms
/// whichever first; DESIGN.md §7 writer budget).
const EGRESS_BATCH_EVENTS: usize = 1_000;
const EGRESS_FLUSH_TICK_MS: u64 = 5;

/// The egress writer: batch staged events into `oms.outbox` rows under one
/// RLS-contexted transaction per env group per flush. The shell's relay
/// (boot 8b) produces the rows to Bus — this task only stages them
/// durably. An insert failure drops that event with an error log (the
/// supervision plane re-decides from current state; the peg mirrors are
/// observer-visible only).
async fn egress_writer(pool: sqlx::PgPool, rx: &mut tokio::sync::mpsc::Receiver<StagedEvent>) {
    loop {
        let mut batch: Vec<StagedEvent> = Vec::with_capacity(EGRESS_BATCH_EVENTS);
        let Some(first) = rx.recv().await else {
            break; // channel closed — stage done
        };
        batch.push(first);
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(EGRESS_FLUSH_TICK_MS);
        while batch.len() < EGRESS_BATCH_EVENTS {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(ev)) => batch.push(ev),
                Ok(None) => break,
                Err(_) => break, // tick expired — flush what we have
            }
        }
        // Group by env (the RLS context is transaction-local — the ME
        // writer's per-env grouping).
        let mut envs: Vec<Uuid> = Vec::new();
        for ev in &batch {
            if !envs.contains(&ev.env_id) {
                envs.push(ev.env_id);
            }
        }
        for env in envs {
            let mut tx = match pool.begin().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!(error = %e, "oms egress writer: tx begin failed — batch dropped");
                    continue;
                }
            };
            sentin8::shell::set_tenant_context(&mut tx, env).await;
            for ev in batch.iter().filter(|e| e.env_id == env) {
                if let Err(e) =
                    crate::events::emit_outbox(&mut tx, &identity_of(ev), &ev.envelope).await
                {
                    tracing::error!(error = %e, "oms egress writer: insert failed");
                }
            }
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "oms egress writer: commit failed");
            }
        }
    }
    tracing::info!("oms egress writer: channel closed — stopped");
}

/// The `EventIdentity` for one staged event (rebuilt for the insert).
fn identity_of(ev: &StagedEvent) -> EventIdentity {
    EventIdentity {
        env_id: ev.env_id,
        subject_id: ev.subject_id.clone(),
        correlation_id: ev.correlation_id.clone(),
        idempotency_key: ev.idempotency_key.clone(),
    }
}

/// Peg registration (the supervisor's intake): inserts the peg into its
/// shard's map + the instrument registry, acks at limit (the `received →`
/// `booked` mirror pair rides the same intake), enforcing the
/// instrument peg cap (design §9).
pub async fn register_peg(
    env_id: Uuid,
    peg_id: u64,
    instrument_id: u32,
    kind: crate::pegs::PegKind,
    side: crate::pegs::PegSide,
    offset: i128,
    limit: exchange8::fixed::Price,
) -> Result<(), &'static str> {
    let s = shard_of(instrument_id, DEFAULT_SHARDS);
    let Some(shards) = shards() else {
        return Err("shard plane not running");
    };
    let Some(rt) = shards.get(&s) else {
        return Err("shard not provisioned");
    };
    let mut shard = rt.lock().await;
    let on_instrument = pegs_on(&shard.pegs, instrument_id).len();
    if on_instrument >= INSTRUMENT_PEG_CAP {
        return Err("instrument peg cap exceeded");
    }
    let peg = crate::peg_machine::PegOrder::new(peg_id, kind, side, offset, limit);
    shard.pegs.insert(peg_id, peg);
    INSTRUMENT_INDEX
        .lock()
        .unwrap()
        .insert(peg_id, instrument_id);
    PEG_ENV.lock().unwrap().insert(peg_id, env_id);
    // The received mirror + the booked-at-limit ack ride the egress stage
    // (the intake is a durable mutation of the supervision surface's
    // client contract; the working-slice actions stage as they decide).
    let identity = EventIdentity {
        env_id,
        subject_id: "exchange8:oms".into(),
        correlation_id: Uuid::new_v4().to_string(),
        idempotency_key: format!("peg-received:{peg_id}"),
    };
    stage_event(StagedEvent {
        env_id,
        envelope: crate::events::peg_event(
            crate::events::PegOp::Received,
            serde_json::json!({
                "peg_id": peg_id,
                "instrument_id": instrument_id,
                "state": "on_hold",
                "limit": limit.raw,
            }),
            &identity,
        ),
        subject_id: identity.subject_id.clone(),
        correlation_id: identity.correlation_id.clone(),
        idempotency_key: identity.idempotency_key.clone(),
    });
    Ok(())
}

/// Spawn the OMS evaluation plane: the per-shard evaluator tasks + the
/// quote consumer + the algo pump. Called from the `with_state` closure
/// (shell boot step 8); the shell's relay (8b) and audit pusher (8c) are
/// already running and drain what the plane stages.
pub fn spawn_tasks(core: Arc<AppState>) {
    let mut runtimes: HashMap<usize, Arc<tokio::sync::Mutex<ShardRuntime>>> = HashMap::new();
    let mut senders: HashMap<usize, exchange8::wire::BoundedSender<ShardInput>> = HashMap::new();
    for s in 0..DEFAULT_SHARDS {
        let (tx, rx) = exchange8::wire::channel::<ShardInput>(4_096);
        let rt = Arc::new(tokio::sync::Mutex::new(ShardRuntime::default()));
        runtimes.insert(s, rt.clone());
        senders.insert(s, tx);
        tokio::spawn(shard_loop(rt, rx));
    }
    let _ = SHARD_RUNTIMES.set(Arc::new(runtimes));
    let _ = SHARD_SENDERS.set(Arc::new(senders));
    // The quote reader (Bus consumer; fail-open per the producer seam).
    {
        let core = Arc::clone(&core);
        tokio::spawn(async move {
            if let Err(e) = quote_consumer(&core).await {
                tracing::error!(error = %e, "oms: quote consumer task exited");
            }
        });
    }
    // The algo scheduler (durable parent binding; fail-open).
    spawn_algo_pump(Arc::clone(&core));
    // The egress writer: drains the stage into `oms.outbox` rows (batched
    // 1000 events / 5 ms — the ME writer budget; DESIGN.md §7). The shell
    // relay (8b) produces them; the audit pusher (8c) journals the paired
    // audit rows.
    let (_, rx) = open_egress_stage();
    let pool = core.pool.clone();
    tokio::spawn(async move {
        let mut guard = rx.lock().await;
        egress_writer(pool, &mut guard).await;
    });
}

/// Project one `PegAction` decision into the egress stage (the peg
/// lifecycle mirror — DESIGN.md §4 vocabulary; the actions map 1:1).
fn project_peg_action(
    peg_id: u64,
    peg: &crate::peg_machine::PegOrder,
    action: &crate::peg_machine::PegAction,
) {
    let instrument_id = instrument_index(peg_id);
    let env_id = peg_environment(peg_id);
    let Some((instrument_id, env_id)) = instrument_id.zip(env_id) else {
        return; // unregistered/unattributed peg — nothing to mirror
    };
    use crate::peg_machine::PegAction;
    let (op, snapshot) = match action {
        PegAction::None => return,
        PegAction::Submit { price } => (
            crate::events::PegOp::Booked,
            serde_json::json!({
                "peg_id": peg_id,
                "instrument_id": instrument_id,
                "state": peg.status_key(),
                "working_price": price.raw,
            }),
        ),
        PegAction::Replace { price } => (
            crate::events::PegOp::Repriced,
            serde_json::json!({
                "peg_id": peg_id,
                "instrument_id": instrument_id,
                "state": peg.status_key(),
                "working_price": price.raw,
                "converted": peg.converted_to_limit,
            }),
        ),
        PegAction::Withdraw => (
            crate::events::PegOp::Held,
            serde_json::json!({
                "peg_id": peg_id,
                "instrument_id": instrument_id,
                "state": peg.status_key(),
            }),
        ),
    };
    let identity = EventIdentity {
        env_id,
        subject_id: "exchange8:oms".into(),
        correlation_id: Uuid::new_v4().to_string(),
        idempotency_key: format!("peg-action:{peg_id}:{}", op_key(&op)),
    };
    stage_event(StagedEvent {
        env_id,
        envelope: crate::events::peg_event(op, snapshot, &identity),
        subject_id: identity.subject_id.clone(),
        correlation_id: identity.correlation_id.clone(),
        idempotency_key: identity.idempotency_key.clone(),
    });
}

fn op_key(op: &crate::events::PegOp) -> &'static str {
    match op {
        crate::events::PegOp::Received => "received",
        crate::events::PegOp::Booked => "booked",
        crate::events::PegOp::Repriced => "repriced",
        crate::events::PegOp::Killed => "killed",
        crate::events::PegOp::Executed => "executed",
        crate::events::PegOp::Held => "held",
    }
}

/// The tenant environment a peg attributes to (`{SERVICE}_ENV_ID` chain —
/// the same resolve the MemBus handles apply, so the events and the
/// publishes share one tenant identity).
fn peg_environment(peg_id: u64) -> Option<Uuid> {
    PEG_ENV.lock().unwrap().get(&peg_id).copied()
}

/// One shard's evaluator loop: drain the bounded queue, apply each input on
/// the owned state; a short sleep between empty drains keeps the loop
/// responsive without a busy spin.
async fn shard_loop(
    rt: Arc<tokio::sync::Mutex<ShardRuntime>>,
    mut rx: exchange8::wire::Stream<ShardInput>,
) {
    loop {
        let mut worked = false;
        while let Some(input) = rx.try_recv() {
            worked = true;
            evaluate(&rt, input).await;
        }
        if !worked {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Quote consumer (Bus → shard queues; DESIGN.md §7 reader task)
// ---------------------------------------------------------------------------

/// The quote event OMS consumes (the declared market-data vocabulary —
/// master design §7.1; payload `exchange8::contracts::events::BboUpdated`).
pub const QUOTE_EVENT: &str = "exchange8:md:bbo:updated";
/// The consumed stream family (the Bus stream key is `bus:{env}:exchange8`
/// — the bare vertical slug every vertical producer uses; the refdata and
/// gateway leaves set the same convention).
pub const QUOTE_STREAM_FAMILY: &str = "exchange8";
/// The consumer group (quote-reader fan-out across instances rides it).
pub const QUOTE_CONSUMER_GROUP: &str = "exchange8-oms-quotes";

/// The quote consumer: batched `POST /v1/consume` (XREADGROUP semantics)
/// against the vertical's stream family, decoding the BBO payload and
/// routing into the instrument shard queues (bounded `try_send`; a full
/// queue drops the stage and the next quote re-drives evaluation — the
/// newest-wins quote semantics). Runs ahead of the producers: a Bus outage
/// or an empty stream idles the loop — pegs hold their last evaluation
/// (quote-stall semantics, DESIGN.md §6).
async fn quote_consumer(core: &Arc<AppState>) -> anyhow::Result<()> {
    // Cert-presenting Bus client (CERTFREE-4V): the `exchange8:oms`
    // workload CERTIFICATE is the identity — no DCR, no token; the
    // mint retries while the fleet boots.
    let bus = loop {
        match sentin8::bus::client::BusClient::for_workload(
            &core.config.sentin8_bus_url,
            &core.config.sentin8_auth_issuer,
            "exchange8:oms",
        )
        .await
        {
            Ok(c) => break c,
            Err(e) => {
                tracing::debug!(error = %e, "oms bus client: certificate not ready yet");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    };
    loop {
        let resp = match bus
            .consume(&sentin8::bus::contracts::ConsumeRequest {
                stream_family: QUOTE_STREAM_FAMILY.into(),
                consumer_group: QUOTE_CONSUMER_GROUP.into(),
                max_count: 500,
                block_ms: 1000,
            })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "oms quote consume unavailable; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        for msg in resp.messages {
            if msg.event_type != QUOTE_EVENT {
                // Non-quote events in the family (peg mirrors, ME
                // lifecycle …) are ACKed and skipped — the evaluator
                // consumes quotes only.
                let _ = bus
                    .ack(QUOTE_STREAM_FAMILY, QUOTE_CONSUMER_GROUP, &msg.message_id)
                    .await;
                continue;
            }
            let payload = match serde_json::from_value::<exchange8::contracts::events::BboUpdated>(
                msg.payload.clone(),
            ) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "oms quote consumer: malformed BBO payload; skipped");
                    let _ = bus
                        .ack(QUOTE_STREAM_FAMILY, QUOTE_CONSUMER_GROUP, &msg.message_id)
                        .await;
                    continue;
                }
            };
            if let Some(senders) = SHARD_SENDERS.get() {
                let s = shard_of(payload.instrument_id, DEFAULT_SHARDS);
                if let Some(tx) = senders.get(&s) {
                    let _ = tx.try_send(ShardInput::Quote {
                        instrument_id: payload.instrument_id,
                        quote: Quote {
                            best_bid: payload.quote.bid,
                            best_ask: payload.quote.ask,
                        },
                    });
                }
            }
            let _ = bus
                .ack(QUOTE_STREAM_FAMILY, QUOTE_CONSUMER_GROUP, &msg.message_id)
                .await;
        }
    }
}

// ---------------------------------------------------------------------------
// Algo scheduler — the durable `oms.algo_order.state` binding (DESIGN.md §3)
// ---------------------------------------------------------------------------

/// The algo scheduler pump cadence (bounded; every step is idempotent per
/// parent — a re-delivered pump hits the guarded insert/state predicate).
const ALGO_PUMP_TICK_MS: u64 = 1_000;

/// The durable algo state vocabulary (the `oms.algo_order.state` CHECK).
pub const ALGO_STATE_RUNNING: &str = "running";
/// `paused` — the plan halted; resumable.
pub const ALGO_STATE_PAUSED: &str = "paused";
/// `complete` — the plan consumed its full parent quantity (terminal).
pub const ALGO_STATE_COMPLETE: &str = "complete";
/// `cancelled` — withdrawn before completion (terminal).
pub const ALGO_STATE_CANCELLED: &str = "cancelled";

/// The durable algo parent counts for the stat gauges (running parents +
/// the slices-issued counter — owner-side env sweep under per-env context,
/// the pump's own maintenance view; §4.3d same-connection rule). A DB
/// outage degrades both to 0 (a gauge, never a control signal).
pub async fn runtime_algo_counts(state: &Arc<AppState>) -> (u64, u64) {
    use sentin8::config::SYSTEM_ENV_ID;
    let envs: Vec<Uuid> =
        sqlx::query_scalar("SELECT DISTINCT env_id FROM sentin8_outbox_unrelayed_envs('oms')")
            .fetch_all(&state.pool)
            .await
            .unwrap_or_else(|_| vec![SYSTEM_ENV_ID]);
    let envs = if envs.is_empty() {
        vec![SYSTEM_ENV_ID]
    } else {
        envs
    };
    let mut running = 0u64;
    let mut slices = 0u64;
    for env_id in envs {
        let Ok(mut tx) = state.pool.begin().await else {
            return (0, 0); // degraded gauge on DB outage
        };
        sentin8::shell::set_tenant_context(&mut tx, env_id).await;
        let Ok(Some((r, s))) = sqlx::query_as::<_, (i64, i64)>(
            r#"SELECT
                 (SELECT count(*) FROM oms.algo_order WHERE env_id = $1 AND state = $2),
                 (SELECT count(*) FROM oms.algo_child_order WHERE env_id = $1)"#,
        )
        .bind(env_id)
        .bind(ALGO_STATE_RUNNING)
        .fetch_optional(&mut *tx)
        .await
        else {
            continue;
        };
        let _ = tx.commit().await;
        running += r.max(0) as u64;
        slices += s.max(0) as u64;
    }
    (running, slices)
}

/// The running parents + total child slices (the durable counts).
async fn algo_counts(state: &Arc<AppState>) -> anyhow::Result<(u64, u64)> {
    let running: i64 = sqlx::query_scalar("SELECT count(*) FROM oms.algo_order WHERE state = $1")
        .bind(ALGO_STATE_RUNNING)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let slices: i64 = sqlx::query_scalar("SELECT count(*) FROM oms.algo_child_order")
        .fetch_one(&state.pool)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok((running.max(0) as u64, slices.max(0) as u64))
}

/// The algo scheduler task (the pump loop; fail-open — a DB outage never
/// blocks the peg/trigger plane; the next tick retries).
fn spawn_algo_pump(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(ALGO_PUMP_TICK_MS)).await;
            if let Err(e) = pump_algo_orders(&state).await {
                tracing::warn!(error = %e, "oms algo pump failed; retrying");
            }
        }
    });
}

/// Advance every RUNNING algo parent one pump step per env: the parent's
/// slice plan derives per pump from the durable row + the child frontier
/// (`oms.algo_child_order.slice_no` — the resume-restart frontier), so the
/// durable state is authoritative across restarts. Every step is ONE
/// transaction (child row + outbox + audit journal).
async fn pump_algo_orders(state: &Arc<AppState>) -> anyhow::Result<u32> {
    use sentin8::config::SYSTEM_ENV_ID;
    // The pump is owner-side maintenance spanning envs — envs resolve from
    // the platform helper's DISTINCT env scan over this schema's outbox
    // (the gateway purge pattern; SECURITY DEFINER, env UUIDs only).
    let envs: Vec<Uuid> =
        sqlx::query_scalar("SELECT DISTINCT env_id FROM sentin8_outbox_unrelayed_envs('oms')")
            .fetch_all(&state.pool)
            .await
            .unwrap_or_else(|e| {
                tracing::debug!(
                    error = %e,
                    "oms algo pump: env scan unavailable (helper not provisioned) — system env only"
                );
                vec![SYSTEM_ENV_ID]
            });
    let mut advanced = 0u32;
    for env_id in envs {
        let parents: Vec<Uuid> = {
            let mut tx = state.pool.begin().await.map_err(|e| anyhow::anyhow!(e))?;
            sentin8::shell::set_tenant_context(&mut tx, env_id).await;
            let rows: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM oms.algo_order WHERE env_id = $1 AND state = $2 LIMIT 100",
            )
            .bind(env_id)
            .bind(ALGO_STATE_RUNNING)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
            tx.commit().await.map_err(|e| anyhow::anyhow!(e))?;
            rows
        };
        for algo_id in parents {
            if issue_algo_step(state, env_id, algo_id).await? {
                advanced += 1;
            }
        }
    }
    Ok(advanced)
}

/// Issue one parent's next pump step: record the child slice (the durable
/// child row is the plan frontier) + the issuance event + the audit row in
/// ONE transaction. A parent without a carried slice size idles (its plan
/// lives entirely on the order-flow path; the pump only parents that
/// carry `slice_size`).
///
/// # Errors
/// Propagates SQL failures (the pump caller retries on its next tick).
async fn issue_algo_step(
    state: &Arc<AppState>,
    env_id: Uuid,
    algo_id: Uuid,
) -> anyhow::Result<bool> {
    let mut tx = state.pool.begin().await.map_err(|e| anyhow::anyhow!(e))?;
    sentin8::shell::set_tenant_context(&mut tx, env_id).await;
    // The parent lock: state + plan (the durable binding decides — paused)
    // complete/cancelled parents idle).
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT state, slice_size::text FROM oms.algo_order WHERE id = $1 AND env_id = $2 FOR UPDATE",
    )
    .bind(algo_id)
    .bind(env_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| anyhow::anyhow!(e))?;
    let Some((state_txt, slice_size)) = row else {
        tx.rollback().await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(false);
    };
    if state_txt != ALGO_STATE_RUNNING {
        tx.rollback().await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(false);
    }
    let frontier: i32 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(slice_no), 0) FROM oms.algo_child_order WHERE env_id = $1 AND algo_order_id = $2",
    )
    .bind(env_id)
    .bind(algo_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    // The pump step: one slice per tick, quantity = `slice_size` (raw qty).
    // A parent with no `slice_size` runs the plan entirely on its order-flow
    // path (the pump leaves it running; the fill sink marks exhaustion).
    let slice_size_raw: i64 = slice_size
        .as_deref()
        .and_then(|s| s.replace(',', "").parse().ok())
        .unwrap_or(0);
    if slice_size_raw <= 0 {
        tx.rollback().await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(false);
    }
    let slice_no = frontier + 1;
    let identity = EventIdentity {
        env_id,
        subject_id: "exchange8:oms".into(),
        correlation_id: algo_id.to_string(),
        idempotency_key: format!("algo-slice:{algo_id}:{slice_no}"),
    };
    // The child row: idempotent per (parent, slice_no) re-delivery — the
    // UNIQUE (env_id, parent_order_id, slice_no) semantics ride the
    // ON CONFLICT DO NOTHING + RETURNING guard here.
    let inserted: Option<(Uuid,)> = sqlx::query_as(
        r#"INSERT INTO oms.algo_child_order (env_id, algo_order_id, child_order_id, slice_no)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT DO NOTHING
           RETURNING id"#,
    )
    .bind(env_id)
    .bind(algo_id)
    .bind(slice_size_raw)
    .bind(slice_no)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| anyhow::anyhow!(e))?;
    let Some((child_row_id,)) = inserted else {
        // A prior pass already recorded this slice (idempotency).
        tx.rollback().await.map_err(|e| anyhow::anyhow!(e))?;
        return Ok(false);
    };
    let envelope = crate::events::algo_event(
        AlgoOp::ChildIssued,
        serde_json::json!({
            "algo_order_id": algo_id,
            "child_row_id": child_row_id,
            "slice_no": slice_no,
            "slice_qty": slice_size_raw,
        }),
        &identity,
    );
    crate::events::emit_outbox(&mut tx, &identity, &envelope)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    crate::events::journal_mutation(
        &mut tx,
        &identity,
        AuditOp::Execute,
        "algo_child_order",
        &child_row_id.to_string(),
        &format!("algo child slice {slice_no} issued for parent {algo_id}"),
        None,
        Some(serde_json::json!({ "slice_no": slice_no, "qty": slice_size_raw })),
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;
    tx.commit().await.map_err(|e| anyhow::anyhow!(e))?;
    Ok(true)
}

/// The guarded durable algo transition (the pause/resume binding): the
/// state predicate makes a re-run a no-op (409 on divergence); the
/// transition event + audit row ride the SAME transaction (DESIGN.md §3).
pub async fn algo_state_transition(
    state: &Arc<AppState>,
    a: &sentin8::shell::AuthContext,
    headers: &axum::http::HeaderMap,
    algo_id: Uuid,
    from: &str,
    to: &str,
    op: AlgoOp,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut conn = state.pool.acquire().await.map_err(sentin8::shell::err_db)?;
    let mut tx = conn.begin().await.map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut tx, a.env_id()).await;
    let current: Option<String> =
        sqlx::query_scalar("SELECT state FROM oms.algo_order WHERE id = $1 AND env_id = $2")
            .bind(algo_id)
            .bind(a.env_id())
            .fetch_optional(&mut *tx)
            .await
            .map_err(sentin8::shell::err_db)?;
    let Some(current) = current else {
        tx.rollback().await.map_err(sentin8::shell::err_db)?;
        return Err((StatusCode::NOT_FOUND, "algo order not found".into()));
    };
    if current != from {
        tx.rollback().await.map_err(sentin8::shell::err_db)?;
        return Err((
            StatusCode::CONFLICT,
            format!("algo order in state '{current}', expected '{from}'"),
        ));
    }
    sqlx::query(
        "UPDATE oms.algo_order SET state = $3 WHERE id = $1 AND env_id = $2 AND state = $4",
    )
    .bind(algo_id)
    .bind(a.env_id())
    .bind(to)
    .bind(from)
    .execute(&mut *tx)
    .await
    .map_err(sentin8::shell::err_db)?;
    let identity = EventIdentity {
        env_id: a.env_id(),
        subject_id: a.claims.sub.clone(),
        correlation_id: correlation_from(headers),
        idempotency_key: format!("algo-{to}:{algo_id}"),
    };
    let envelope = crate::events::algo_event(
        op,
        serde_json::json!({
            "algo_order_id": algo_id,
            "state": to,
            "previous_state": from,
        }),
        &identity,
    );
    crate::events::emit_outbox(&mut tx, &identity, &envelope)
        .await
        .map_err(sentin8::shell::err_db)?;
    crate::events::journal_mutation(
        &mut tx,
        &identity,
        AuditOp::Update,
        "algo_order",
        &algo_id.to_string(),
        &format!("algo parent {from} → {to}"),
        Some(serde_json::json!({ "state": from })),
        Some(serde_json::json!({ "state": to })),
    )
    .await
    .map_err(sentin8::shell::err_db)?;
    tx.commit().await.map_err(sentin8::shell::err_db)?;
    Ok(Json(
        serde_json::json!({ "id": algo_id.to_string(), "state": to }),
    ))
}

// ---------------------------------------------------------------------------
// Admin RFW handlers (declared pages + data endpoints + algo surface)
// ---------------------------------------------------------------------------

/// The correlation id from the platform middleware (`X-Sentin8-Correlation-
/// Id`; a missing header — direct CLI/dev calls — generates one so every
/// envelope stays complete).
pub(crate) fn correlation_from(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-sentin8-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// `GET /v1/admin/ui/{page}` — the declared RFW page definition (Dash
/// renders it; gated by the page's Shield action; 404 for undeclared
/// slugs) — the refdata `handlers::admin_ui_page` pattern.
pub async fn admin_ui_page(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(page): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.config, &headers).await?;
    let definition = admin_pages::oms_pages()
        .into_iter()
        .find(|p| p.slug == page)
        .ok_or((StatusCode::NOT_FOUND, "unknown admin page".into()))?;
    sentin8::shell::policy::require(
        &state.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new(definition.scope),
    )
    .await
    .map(|_| ())?;
    Ok(Json(definition.to_rfw()))
}

/// One live peg row (the `pegs` page table data; memory-resident
/// supervision state projected for the RFW table).
#[derive(Debug, Serialize)]
pub struct PegRow {
    /// Peg id.
    pub peg_id: u64,
    /// Lifecycle state (the peg machine's status key).
    pub state: String,
    /// Working price (raw ticks; null when not working).
    pub working_price: Option<String>,
    /// The peg's limit (raw ticks).
    pub limit_price: String,
    /// Whether the peg converted to a plain limit order.
    pub converted: bool,
}

/// `POST /v1/admin/pegs` — the live peg table (RFW page data; read-only).
/// Gate: the `pegs` page's read scope.
pub async fn admin_pegs(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<PegRow>>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new(admin_pages::PEGS_SCOPE),
    )
    .await
    .map(|_| ())?;
    let _ = &a;
    let mut rows = Vec::new();
    if let Some(shards) = shards() {
        for rt in shards.values() {
            let shard = rt.lock().await;
            for (peg_id, peg) in &shard.pegs {
                rows.push(PegRow {
                    peg_id: *peg_id,
                    state: peg.status_key().to_string(),
                    working_price: peg.working().map(|p| p.raw.to_string()),
                    limit_price: peg.limit.raw.to_string(),
                    converted: peg.converted_to_limit,
                });
            }
        }
    }
    rows.sort_by_key(|r| r.peg_id);
    Ok(Json(rows))
}

/// One held conditional row (the `conditionals` page table data).
#[derive(Debug, Serialize)]
pub struct ConditionalRow {
    /// Order id.
    pub order_id: u64,
    /// Instrument id.
    pub instrument_id: u32,
    /// Trigger reference (last/bid/ask/index).
    pub trigger_ref: String,
    /// Trigger threshold (raw ticks).
    pub trigger_price: i128,
    /// OCO/OCA group (null when ungrouped).
    pub oco_group: Option<u64>,
}

/// `POST /v1/admin/conditionals` — the held conditional table (RFW page
/// data; read-only). Gate: the `conditionals` page's read scope.
pub async fn admin_conditionals(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<ConditionalRow>>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new(admin_pages::CONDITIONALS_SCOPE),
    )
    .await
    .map(|_| ())?;
    let mut rows = Vec::new();
    if let Some(shards) = shards() {
        for rt in shards.values() {
            let shard = rt.lock().await;
            for o in shard.triggers.held() {
                rows.push(ConditionalRow {
                    order_id: o.order_id,
                    instrument_id: o.instrument_id,
                    trigger_ref: format!("{:?}", o.trigger_ref),
                    trigger_price: o.trigger_price,
                    oco_group: o.oco_group,
                });
            }
        }
    }
    rows.sort_by_key(|r| r.order_id);
    Ok(Json(rows))
}

/// One durable algo parent row (the `algos` page table data).
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct AlgoOrderRow {
    /// Row id.
    pub id: Uuid,
    /// Parent order id.
    pub parent_order_id: i64,
    /// Algo type (the CHECK vocabulary).
    pub algo_type: String,
    /// Durable lifecycle state (the CHECK vocabulary).
    pub state: String,
    /// Slice size (raw qty text).
    pub slice_size: Option<String>,
}

/// `POST /v1/admin/algo-orders` — the durable algo parents (RFW page data;
/// RLS-scoped read of `oms.algo_order` with explicit `env_id`). Gate: the
/// `algos` read scope.
pub async fn admin_algo_orders(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<AlgoOrderRow>>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new(admin_pages::ALGOS_SCOPE),
    )
    .await
    .map(|_| ())?;
    let mut conn = state.pool.acquire().await.map_err(sentin8::shell::err_db)?;
    let mut ctx = conn.begin().await.map_err(sentin8::shell::err_db)?;
    sentin8::shell::set_tenant_context(&mut ctx, a.env_id()).await;
    let rows = sqlx::query_as::<_, AlgoOrderRow>(
        r#"SELECT id, parent_order_id, algo_type, state, slice_size::text AS slice_size
             FROM oms.algo_order
            WHERE env_id = $1
            ORDER BY id
            LIMIT 200"#,
    )
    .bind(a.env_id())
    .fetch_all(&mut *ctx)
    .await
    .map_err(sentin8::shell::err_db)?;
    ctx.commit().await.map_err(sentin8::shell::err_db)?;
    Ok(Json(rows))
}

/// `POST /v1/admin/algo-orders/{id}/pause` — pause the running parent
/// (`running → paused`; the durable binding + transition event + audit).
pub async fn pause_algo_order(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(algo_id): axum::extract::Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    pause_resume(state, headers, algo_id, ALGO_STATE_PAUSED, AlgoOp::Paused).await
}

/// `POST /v1/admin/algo-orders/{id}/resume` — resume the paused parent
/// (`paused → running`).
pub async fn resume_algo_order(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(algo_id): axum::extract::Path<Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    pause_resume(state, headers, algo_id, ALGO_STATE_RUNNING, AlgoOp::Resumed).await
}

/// Shared pause/resume surface: verify → gate (the `algos` write action) →
/// the guarded durable transition.
async fn pause_resume(
    state: Arc<AppState>,
    headers: axum::http::HeaderMap,
    algo_id: Uuid,
    to: &str,
    op: AlgoOp,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new(admin_pages::ALGOS_WRITE_SCOPE),
    )
    .await
    .map(|_| ())?;
    let from = if to == ALGO_STATE_PAUSED {
        ALGO_STATE_RUNNING
    } else {
        ALGO_STATE_PAUSED
    };
    algo_state_transition(&state, &a, &headers, algo_id, from, to, op).await
}

// ---------------------------------------------------------------------------
// RFW page definitions (refdata `admin_ui.rs` pattern — pure data)
// ---------------------------------------------------------------------------

/// The RFW page definitions (the registration-declared pages + the algo
/// surface). Icons are Material Icons code points (hex), never names.
pub mod admin_pages {
    use serde_json::{json, Value};

    /// The `pegs` page's Shield action (the registration admin vocabulary).
    pub const PEGS_SCOPE: &str = "exchange8:order:read";
    /// The `conditionals` page's Shield action.
    pub const CONDITIONALS_SCOPE: &str = "exchange8:order:read";
    /// The `algos` page's Shield action (read).
    pub const ALGOS_SCOPE: &str = "exchange8:order:read";
    /// The algo pause/resume mutation's Shield action (the write vocabulary
    /// the mirror covers).
    pub const ALGOS_WRITE_SCOPE: &str = "exchange8:order:write";

    /// One RFW page definition (the shape Dash renders).
    #[derive(Clone, Debug, PartialEq)]
    pub struct RfwPage {
        /// Route segment: served at `/v1/admin/ui/{slug}`.
        pub slug: &'static str,
        /// Page title.
        pub title: &'static str,
        /// Material Icons code point (hex string, not a name).
        pub icon_hex: &'static str,
        /// Required admin scope.
        pub scope: &'static str,
        /// Sections (RFW blocks: tables, actions).
        pub sections: Vec<RfwSection>,
    }

    /// One RFW section.
    #[derive(Clone, Debug, PartialEq)]
    pub enum RfwSection {
        /// Read-only table bound to a resource.
        Table {
            /// Section heading.
            heading: &'static str,
            /// The data resource the table renders (`/v1/admin/{resource}`).
            resource: &'static str,
            /// Columns (payload key, display label).
            columns: Vec<(&'static str, &'static str)>,
        },
        /// Action button bound to a POST route.
        Action {
            /// Section heading.
            heading: &'static str,
            /// The POST resource (the renderer only supports POST).
            resource: &'static str,
            /// Button label.
            label: &'static str,
            /// Inputs (payload key, display label).
            fields: Vec<(&'static str, &'static str)>,
        },
    }

    impl RfwPage {
        /// The RFW definition JSON served at `GET /v1/admin/ui/{slug}`.
        pub fn to_rfw(&self) -> Value {
            let sections: Vec<Value> = self
                .sections
                .iter()
                .map(|s| match s {
                    RfwSection::Table {
                        heading,
                        resource,
                        columns,
                    } => json!({
                        "type": "table",
                        "heading": heading,
                        "resource": resource,
                        "columns": columns.iter()
                            .map(|(k, l)| json!({"key": k, "label": l}))
                            .collect::<Vec<_>>(),
                    }),
                    RfwSection::Action {
                        heading,
                        resource,
                        label,
                        fields,
                    } => json!({
                        "type": "action",
                        "heading": heading,
                        "resource": resource,
                        "label": label,
                        "fields": fields.iter()
                            .map(|(k, l)| json!({"key": k, "label": l}))
                            .collect::<Vec<_>>(),
                    }),
                })
                .collect();
            json!({
                "slug": self.slug,
                "title": self.title,
                "icon": self.icon_hex,
                "scope": self.scope,
                "sections": sections,
            })
        }
    }

    /// The OMS admin pages — the registration-declared set (`pegs`,
    /// `conditionals`) plus the `algos` surface (the durable parent
    /// binding's admin view + the pause/resume actions).
    pub fn oms_pages() -> Vec<RfwPage> {
        vec![
            RfwPage {
                slug: "pegs",
                title: "Pegs",
                icon_hex: "0xe905",
                scope: PEGS_SCOPE,
                sections: vec![RfwSection::Table {
                    heading: "Working pegs",
                    resource: "pegs",
                    columns: vec![
                        ("peg_id", "Peg"),
                        ("state", "State"),
                        ("working_price", "Working"),
                        ("limit_price", "Limit"),
                        ("converted", "Converted"),
                    ],
                }],
            },
            RfwPage {
                slug: "conditionals",
                title: "Conditionals",
                icon_hex: "0xe8b5",
                scope: CONDITIONALS_SCOPE,
                sections: vec![RfwSection::Table {
                    heading: "Held conditional orders",
                    resource: "conditionals",
                    columns: vec![
                        ("order_id", "Order"),
                        ("instrument_id", "Instrument"),
                        ("trigger_ref", "Reference"),
                        ("trigger_price", "Trigger"),
                        ("oco_group", "Group"),
                    ],
                }],
            },
            RfwPage {
                slug: "algos",
                title: "Algo orders",
                icon_hex: "0xe626",
                scope: ALGOS_SCOPE,
                sections: vec![
                    RfwSection::Table {
                        heading: "Algo parents",
                        resource: "algo-orders",
                        columns: vec![
                            ("id", "Parent"),
                            ("algo_type", "Type"),
                            ("state", "State"),
                            ("slice_size", "Slice"),
                        ],
                    },
                    RfwSection::Action {
                        heading: "Pause parent",
                        resource: "algo-orders",
                        label: "Pause",
                        fields: vec![("id", "Algo order ID")],
                    },
                    RfwSection::Action {
                        heading: "Resume parent",
                        resource: "algo-orders",
                        label: "Resume",
                        fields: vec![("id", "Algo order ID")],
                    },
                ],
            },
        ]
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn declared_pages_render_with_scope_icon_sections() {
            let pages = oms_pages();
            // The registration declares exactly these routes.
            let slugs: Vec<&str> = pages.iter().map(|p| p.slug).collect();
            assert_eq!(slugs, vec!["pegs", "conditionals", "algos"]);
            for page in pages {
                let v = page.to_rfw();
                assert_eq!(v["slug"], page.slug);
                // Icons are Material code points — hex strings (the `0x`
                // prefix is the registration payload's spelling).
                let icon = v["icon"].as_str().unwrap();
                assert!(
                    icon.trim_start_matches("0x")
                        .chars()
                        .all(|c| c.is_ascii_hexdigit()),
                    "icon must be a Material code point (hex): {icon}"
                );
                assert!(v["scope"].as_str().unwrap().starts_with("exchange8:"));
                assert!(!v["sections"].as_array().unwrap().is_empty());
            }
        }

        #[test]
        fn action_resources_are_post_friendly() {
            // The RFW renderer only supports POST — actions never name
            // PUT/DELETE.
            for page in oms_pages() {
                for s in &page.sections {
                    if let RfwSection::Action { resource, .. } = s {
                        assert!(!resource.contains("PUT"));
                        assert!(!resource.contains("DELETE"));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_assignment_matches_the_me_hash() {
        // Same instrument → same shard, always (ME parity — §7).
        assert_eq!(shard_of(7, DEFAULT_SHARDS), shard_of(7, DEFAULT_SHARDS));
        assert_eq!(shard_of(3, DEFAULT_SHARDS), shard_of(67, DEFAULT_SHARDS));
        assert_eq!(shard_of(999, 1), 0);
    }

    #[test]
    fn algo_state_vocabulary_matches_the_schema_check() {
        // The durable binding's vocabulary is exactly the CHECK's.
        assert_eq!(ALGO_STATE_RUNNING, "running");
        assert_eq!(ALGO_STATE_PAUSED, "paused");
        assert_eq!(ALGO_STATE_COMPLETE, "complete");
        assert_eq!(ALGO_STATE_CANCELLED, "cancelled");
    }

    #[test]
    fn quote_event_vocabulary_follows_the_master_design() {
        // The consumed family + event name match the master design §7.1
        // table row (the consolidated BBO vocabulary).
        assert_eq!(QUOTE_EVENT, "exchange8:md:bbo:updated");
        assert_eq!(QUOTE_EVENT.split(':').count(), 4);
        assert_eq!(QUOTE_STREAM_FAMILY, "exchange8");
        assert!(
            sentin8::bus::outbox::validate_stream_family(QUOTE_STREAM_FAMILY).is_ok(),
            "colons are structurally impossible in a stream family"
        );
        assert!(
            sentin8::bus::outbox::validate_stream_family(
                QUOTE_CONSUMER_GROUP.replace('-', "").as_str()
            )
            .is_ok(),
            "consumer group charset (the ident rules without the reserved-prefix check)"
        );
    }

    #[tokio::test]
    async fn evaluation_plane_routes_quotes_and_stages_mirrors() {
        // Boot the plane in this test process (64 shards + the consumer
        // task — the consumer idles against the disabled bus URL).
        let config = Arc::new(sentin8::shell::ShellConfig {
            service_type: "exchange8:oms",
            port: 0,
            database_url: "postgresql://invalid:invalid@127.0.0.1:5/none".into(),
            sentin8_auth_issuer: "https://auth.invalid".into(),
            sentin8_bus_url: String::new(),
            sentin8_auth_client: {
                let _ = sentin8::tls::crypto::sentin8_pq_provider().install_default();
                sentin8::auth::client::Client::new(
                    sentin8::auth::client::Client::builder()
                        .base_url("https://auth.invalid")
                        .build()
                        .expect("client options"),
                )
            },
            valkey_url: String::new(),
        });
        let pool = sqlx::PgPool::connect_lazy("postgresql://invalid:invalid@127.0.0.1:5/none")
            .expect("lazy pool");
        let core = Arc::new(sentin8::shell::AppState {
            config,
            pool: pool.clone(),
            admin: sentin8::shield::admin::SharedAdminState::test("exchange8:oms", pool),
        });
        spawn_tasks(Arc::clone(&core));
        assert!(shards().is_some(), "shard plane provisioned");

        // Register a peg on instrument 7.
        let env = uuid::Uuid::new_v4();
        register_peg(
            env,
            5001,
            7,
            crate::pegs::PegKind::Primary,
            crate::pegs::PegSide::Bid,
            100,
            exchange8::fixed::Price::equity(10_500),
        )
        .await
        .expect("peg registers under the cap");

        // The peg sits in its shard (the same assignment the ME hash gives 7).
        let s = shard_of(7, DEFAULT_SHARDS);
        let shards = shards().unwrap();
        let shard = shards.get(&s).unwrap().lock().await;
        assert_eq!(shard.pegs.len(), 1);
        assert_eq!(shard.pegs.values().next().unwrap().status_key(), "on_hold");
        drop(shard);

        // Feed a quote into the shard: the peg re-evaluates (OnHold →
        // Executing: Primary bid = bid + offset) and the mirror stages.
        let tx = SHARD_SENDERS.get().unwrap().get(&s).unwrap().clone();
        tx.try_send(ShardInput::Quote {
            instrument_id: 7,
            quote: Quote {
                best_bid: Some(exchange8::fixed::Price::equity(9_900)),
                best_ask: Some(exchange8::fixed::Price::equity(10_100)),
            },
        })
        .expect("shard queue accepts");
        // The evaluator owns the state; wait for the pass.
        for _ in 0..40 {
            let shard = shards.get(&s).unwrap().lock().await;
            if shard.pegs.values().next().unwrap().status_key() == "executing" {
                assert_eq!(
                    shard.pegs.values().next().unwrap().working().map(|p| p.raw),
                    Some(10_000),
                    "Primary bid = bid + offset = 9_900 + 100"
                );
                return;
            }
            drop(shard);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("peg did not re-evaluate to Executing from the quote");
    }
}
