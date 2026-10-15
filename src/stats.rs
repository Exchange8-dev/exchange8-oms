// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS stat emission (Sentin8:Stat measure surface — DESIGN.md §10).
//!
//! The shell's policy boot (`sentin8::shield::runtime::boot_full`) initializes
//! the platform stat emitter per process (the `{leaf}-policy` M2M workload
//! with the `sentin8:stat:measure` scope — `registration.rs::M2M_GRANTS`
//! declares it); this module adds only the OMS-specific measures on top:
//!
//! - `oms.pegs.working` — live working-peg gauge (the shard plane's
//!   supervision surface; sampled by the pump tick).
//! - `oms.conditional.held` — held conditional orders gauge.
//! - `oms.algo.running` — running algo parents gauge (the durable binding's
//!   live set).
//! - `oms.algo.slices.issued` — counter (per pump advance).
//!
//! Fire-and-forget (`emit_metric_event` — a disabled emitter is a no-op, an
//! outage degrades to log-only; the policy design's emission contract).
//!
//! Design: `services/oms/DESIGN.md` §10 (stat-emission wiring);
//! precedent: `sentin8/services/func/src/handlers.rs` (FUNC-5 emitter).

use std::sync::Arc;

use sentin8::shell::AppState;

/// Emit the OMS supervision gauges + counters (one metric event per pass —
/// the pump/evaluator cadence; `metrics` values must be NUMERIC per the
/// stat ingest contract).
pub fn emit_gauges(working: u64, held: u64, running: u64, slices_issued: u64) {
    sentin8::shield::stat_emit::emit_metric_event(
        "exchange8:oms",
        "oms.supervision",
        serde_json::json!({
            "pegs.working": working,
            "conditional.held": held,
            "algo.running": running,
            "algo.slices.issued": slices_issued,
        }),
        serde_json::json!({}),
    );
}

/// Sample + emit the gauges on a cadence (the scheduler task; a disabled
/// emitter is a no-op — spawn is unconditional).
pub fn spawn_stat_sampler(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            let (working, held) = supervision_gauges();
            let (running, slices) = crate::runtime::runtime_algo_counts(&state).await;
            emit_gauges(working, held, running, slices);
        }
    });
}

/// The in-memory supervision gauges (the shard plane's live surface).
fn supervision_gauges() -> (u64, u64) {
    let mut working = 0u64;
    let mut held = 0u64;
    if let Some(shards) = crate::runtime::shards() {
        for rt in shards.values() {
            // try_lock: the sampler never contends with the evaluator (a
            // busy shard skips this sample — a gauge, not a control signal).
            if let Ok(shard) = rt.try_lock() {
                working += shard.pegs.len() as u64;
                held += shard.triggers.held().len() as u64;
            }
        }
    }
    (working, held)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gauges_emit_without_init_are_noop() {
        // The disabled/before-init emitter is a no-op (the policy emitter's
        // contract) — the call must not panic.
        emit_gauges(1, 2, 3, 4);
    }

    #[test]
    fn supervision_gauges_read_the_shard_plane_or_zero() {
        // Pre-boot (no shard plane): both gauges are 0 — never a panic.
        let (working, held) = supervision_gauges();
        assert_eq!((working, held), (0, 0));
    }
}
