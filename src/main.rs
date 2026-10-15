// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:OMS service binary — peg/iceberg supervision + stop/conditional triggers.
//!
//! Design: `design/sentin8-exchange8-oms-technical-design.md` (see the
//! service doc for the exact file). Core logic in the sibling modules;
//! this shell wires the tokio HTTP/TLS surface via the platform
//! `sentin8-shell` crate (registration, DB pool, health). The instrument-
//! sharded evaluation plane (quote/lifecycle readers → per-instrument peg
//! supervisors + trigger engine) spawns at boot (`runtime.rs::spawn_tasks`)
//! — DESIGN.md §7 task graph.

#![allow(dead_code)]

// Global allocator — mimalloc for every exchange8 service binary
// (master design §11.1). Workers (bus consumers, session
// supervisors, purge jobs) are in-process tokio tasks, so the
// binary-level allocator covers them all.
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod events;
mod order_types;
mod peg_machine;
mod pegs;
mod registration;
mod runtime;
mod stats;
mod triggers;

fn extend_router(
    app: axum::Router<std::sync::Arc<sentin8::shell::AppState>>,
    _state: std::sync::Arc<sentin8::shell::AppState>,
) -> axum::Router<std::sync::Arc<sentin8::shell::AppState>> {
    // ── Sentin8:Policy admin API (shared implementation;
    // the shell AppState already implements HasPolicyAdmin).
    sentin8::shell::mount_policy_admin(
        // Peg/trigger supervision runs on event streams from Gateway/ME
        // (design §4). The HTTP router carries health + admin endpoints +
        // the declared RFW pages/data endpoints + the algo pause/resume
        // surface (DESIGN.md §4).
        app.route(
            "/v1/admin/ui/{page}",
            axum::routing::get(runtime::admin_ui_page),
        )
        // Data endpoints (RFW data fetch + POST aliases — the renderer
        // only supports POST).
        .route("/v1/admin/pegs", axum::routing::post(runtime::admin_pegs))
        .route(
            "/v1/admin/conditionals",
            axum::routing::post(runtime::admin_conditionals),
        )
        .route(
            "/v1/admin/algo-orders",
            axum::routing::post(runtime::admin_algo_orders),
        )
        .route(
            "/v1/admin/algo-orders/{id}/pause",
            axum::routing::post(runtime::pause_algo_order),
        )
        .route(
            "/v1/admin/algo-orders/{id}/resume",
            axum::routing::post(runtime::resume_algo_order),
        ),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("exchange8:oms", "EXCHANGE8_OMS")
            .with_registration(registration::print)
            .with_state(|core| async move {
                // The instrument-sharded evaluation plane (DESIGN.md §7):
                // one reader task per upstream stream (quote events from
                // MD/VFeeds via the Bus consumer; peg/iceberg/algo inputs
                // via the in-process supervisor registry) + one evaluation
                // task per instrument shard; the egress plane is the
                // shell's relay/pusher (boot 8b/8c) — this never
                // duplicates them.
                let state = std::sync::Arc::new(core);
                crate::runtime::spawn_tasks(state.clone());
                crate::stats::spawn_stat_sampler(state.clone());
                Ok(state)
            })
            .with_router(extend_router),
    )
    .await
}
