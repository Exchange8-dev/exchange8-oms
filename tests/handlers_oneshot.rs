// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Ex8:OMS HTTP handler tests (DESIGN.md §9 — the router shape).
//!
//! Drives the REAL router through `tower::ServiceExt::oneshot`: route
//! mounting, the authorization pipeline (bearer → Shield step 5), status
//! codes, and the JSON error envelope — without a database. DB-free
//! guarantee: every data-path handler runs `verify_token` (steps 1–4)
//! BEFORE the first pool access, so a bearer-less request never reaches
//! `state.pool`.
//!
//! The OMS crate is bin-only, so the handler modules are pulled in via
//! `#[path]` — the same pattern as the gateway/refdata handler tests. The
//! included modules keep their dead-code allows (test-binary inclusion
//! compiles the whole module set; only the router surface is exercised).

#![allow(dead_code)]

#[path = "../src/events.rs"]
mod events;
#[path = "../src/order_types.rs"]
mod order_types;
#[path = "../src/peg_machine.rs"]
mod peg_machine;
#[path = "../src/pegs.rs"]
mod pegs;
#[path = "../src/registration.rs"]
mod registration;
#[path = "../src/runtime.rs"]
mod runtime;
#[path = "../src/triggers.rs"]
mod triggers;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

/// Minimal router mirroring `main.rs::extend_router` (same paths/methods).
fn router() -> axum::Router {
    use axum::routing::{get, post};
    axum::Router::new()
        // The RFW page definitions (Dash renders them).
        .route("/v1/admin/ui/{page}", get(runtime::admin_ui_page))
        // Data endpoints (RFW data fetch + POST aliases).
        .route("/v1/admin/pegs", post(runtime::admin_pegs))
        .route("/v1/admin/conditionals", post(runtime::admin_conditionals))
        .route("/v1/admin/algo-orders", post(runtime::admin_algo_orders))
        .route(
            "/v1/admin/algo-orders/{id}/pause",
            post(runtime::pause_algo_order),
        )
        .route(
            "/v1/admin/algo-orders/{id}/resume",
            post(runtime::resume_algo_order),
        )
        .with_state(test_state())
}

/// Install the platform's PQ TLS provider (services do this at boot;
/// reqwest inside the auth client requires a configured provider).
fn install_crypto() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = sentin8::tls::crypto::sentin8_pq_provider().install_default();
    });
}

/// `ServiceCore` with a never-connected pool. Safe: the bearer-less
/// requests these tests send are rejected by `verify_bearer_token` before
/// any pool access.
fn test_state() -> Arc<sentin8::shell::AppState> {
    install_crypto();
    let config = Arc::new(sentin8::shell::ShellConfig {
        service_type: "exchange8:oms",
        port: 0,
        database_url: "postgresql://invalid:invalid@127.0.0.1:5/none".into(),
        sentin8_auth_issuer: "https://auth.invalid".into(),
        sentin8_bus_url: String::new(),
        sentin8_auth_client: sentin8::auth::client::Client::new(
            sentin8::auth::client::Client::builder()
                .base_url("https://auth.invalid")
                .build()
                .expect("client options are valid"),
        ),
        valkey_url: String::new(),
    });
    let pool = sqlx::PgPool::connect_lazy("postgresql://invalid:invalid@127.0.0.1:5/none")
        .expect("lazy pool url is parseable");
    Arc::new(sentin8::shell::AppState {
        config,
        pool: pool.clone(),
        admin: sentin8::shield::admin::SharedAdminState::test("exchange8:oms", pool),
    })
}

fn req(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "exchange8-test")
        .body(Body::empty())
        .unwrap()
}

/// 1 — every route rejects a bearer-less request with 401 (or the JSON
/// extractor's 415 on POST routes — the auth pipeline before any DB work).
#[tokio::test]
async fn every_route_rejects_unauthenticated_with_401() {
    let app = router();
    let id = "00000000-0000-0000-0000-0000000000aa";
    for (method, uri) in [
        ("GET", "/v1/admin/ui/pegs"),
        ("GET", "/v1/admin/ui/conditionals"),
        ("GET", "/v1/admin/ui/algos"),
        ("POST", "/v1/admin/pegs"),
        ("POST", "/v1/admin/conditionals"),
        ("POST", "/v1/admin/algo-orders"),
        ("POST", &format!("/v1/admin/algo-orders/{id}/pause")),
        ("POST", &format!("/v1/admin/algo-orders/{id}/resume")),
    ] {
        let res = app.clone().oneshot(req(method, uri)).await.unwrap();
        let st = res.status();
        assert!(
            st == StatusCode::UNAUTHORIZED || st == StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{method} {uri}: expected 401 or 415, got {st}"
        );
    }
}

/// 2 — wrong-method on an existing path → 405 (route table shape check).
#[tokio::test]
async fn wrong_method_gets_405_not_404() {
    let app = router();
    let res = app.oneshot(req("DELETE", "/v1/admin/pegs")).await.unwrap();
    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// 3 — unmatched path → 404.
#[tokio::test]
async fn unknown_path_gets_404() {
    let app = router();
    let res = app.oneshot(req("GET", "/v1/nope")).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

/// 4 — error envelope: plain-text auth message (never SQL internals).
#[tokio::test]
async fn unauthenticated_error_envelope_is_plain_text_401() {
    let app = router();
    let res = app.oneshot(req("GET", "/v1/admin/ui/pegs")).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("missing Authorization header"),
        "error body must carry the auth message, got: {text}"
    );
}

/// 5 — the RFW page definitions mount: bearer-less requests on every slug
/// (declared or not) hit the auth gate BEFORE the route/page resolution.
#[tokio::test]
async fn admin_ui_pages_are_mounted() {
    let app = router();
    for slug in ["pegs", "conditionals", "algos", "nope"] {
        let res = app
            .clone()
            .oneshot(req("GET", &format!("/v1/admin/ui/{slug}")))
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "page '{slug}': gate before the route lookup"
        );
    }
}

/// 6 — the registration's declared admin_ui pages match the served set.
#[test]
fn registration_admin_ui_pages_match_the_router() {
    // `--print-registration` declares `pegs` / `conditionals` + the algos
    // surface; the runtime's page set matches 1:1.
    let declared: Vec<&str> = registration::ADMIN_UI_PAGES
        .iter()
        .map(|p| p.route)
        .collect();
    let served: Vec<&str> = runtime::admin_pages::oms_pages()
        .into_iter()
        .map(|p| p.slug)
        .collect();
    assert_eq!(declared, served);
}

/// 7 — the event vocabulary constants stay inside the vertical vocabulary.
#[test]
fn oms_event_names_follow_the_vertical_format() {
    for name in [
        events::EV_PEG_RECEIVED,
        events::EV_PEG_BOOKED,
        events::EV_CONDITIONAL_RELEASED,
        events::EV_ALGO_CHILD_ISSUED,
    ] {
        let segs: Vec<&str> = name.split(':').collect();
        assert_eq!(segs.len(), 4, "{name}: 4 segments");
        assert_eq!(segs[0], "exchange8");
        assert_eq!(segs[1], "oms");
    }
}
