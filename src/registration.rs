// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
// SPDX-License-Identifier: Apache-2.0

//! Service self-registration data for Exchange8:OMS (generated; hand-tune freely).
//!
//! Output via `--print-registration` (see the platform
//! `sentin8-service-self-registration-design.md`).

use sentin8::registration::{
    AdminScope, AdminUi, AdminUiPage, ErrorCode, M2mGrant, PgRole, PolicySetDecl, Registration,
    RelationDecl, SqlMigration,
};

/// The Shield text of the declared scope-mirror set — public so the
/// policy wiring tests (`tests/policy.rs`) assert against the REAL seed.
///
/// Coverage contract (policy wiring tests): every action the handlers
/// evaluate has a permit — the order-routing route action, the algo
/// pause/resume write action, and the platform-admin trio rides every
/// surface.
pub const SCOPE_MIRROR_POLICIES: &str = "permit(principal, action == Action::\"exchange8:order:route\", resource) when { principal.scopes.contains(\"exchange8:order:route\") };
permit(principal, action == Action::\"exchange8:order:route\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };
permit(principal, action == Action::\"exchange8:order:route\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };
permit(principal, action == Action::\"exchange8:order:route\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };
permit(principal, action == Action::\"exchange8:algo:write\", resource) when { principal.scopes.contains(\"exchange8:algo:write\") };
permit(principal, action == Action::\"exchange8:algo:write\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };
permit(principal, action == Action::\"exchange8:algo:write\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };
permit(principal, action == Action::\"exchange8:algo:write\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };";

/// Default declarative policy set (Sentin8:Policy design §3.2/§3.4,
/// seeded by `vertical enroll` as `origin='platform'`, `mode='enforced'`):
/// action-granular scope permits over the LIVE effective scope set — the
/// exact mirror of the gates this service's handlers evaluate in step 5
/// (policy design §4.3). Enforced mode: step 5 is the only
/// gate; shadow is the emergency one-UPDATE rollback lever only.
const POLICIES: &[PolicySetDecl] = &[PolicySetDecl {
    policy_set_name: "scope-mirror",
    description: "Action-granular scope permits for exchange8:oms over the LIVE effective scope set. Enforced — step 5 is the only gate.",
    // Shield text lives in SCOPE_MIRROR_POLICIES (single source).
    policies: SCOPE_MIRROR_POLICIES,
    schema: None,
    mode: "enforced",
    default_enabled: true,
}];

/// Default ReBAC relation tuples (design §3.1c) — none declared by this
/// service today; env admins author their own graph via the relations
/// admin API.
const RELATIONS: &[RelationDecl] = &[];

const PG_ROLES: &[PgRole] = &[PgRole {
    scope: "exchange8:order:read,exchange8:order:write",
    role: "exchange8_oms_service",
    bypass_rls: false,
    // Grants live in the exchange8 vertical database only (principles §6.6
    // database-per-vertical, schema-per-service).
    database: "exchange8",
    // This service owns all its reads — no cross-schema grants.
    cross_schema_grants: &[],
}];

const ADMIN_SCOPES: &[AdminScope] = &[
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:order:read",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:order:write",
    },
    AdminScope::Typed {
        role: "exchange8:admin",
        scope: "exchange8:algo:write",
    },
    // Policy-admin surface (policy design §3.6; principles §6.0b per-service scope).
    AdminScope::Typed {
        role: "exchange8:oms:admin",
        scope: "exchange8:oms:policy:read",
    },
    AdminScope::Typed {
        role: "exchange8:oms:admin",
        scope: "exchange8:oms:policy:write",
    },
];

const ERROR_CODES: &[ErrorCode] = &[ErrorCode {
    enum_name: "Exchange8TriggerAlreadyFired",
    error_code: "exchange8_trigger_already_fired",
    status_code: 409,
}];

/// The declared RFW pages (public so the router tests assert the served
/// set matches 1:1 — see `tests/handlers_oneshot.rs`).
pub(crate) const ADMIN_UI_PAGES: &[AdminUiPage] = &[
    AdminUiPage {
        route: "pegs",
        label: "Pegs",
        ui_endpoint: "/v1/admin/ui/pegs",
        data_endpoint: "/v1/admin/pegs",
        icon: "0xe905",
    },
    AdminUiPage {
        route: "conditionals",
        label: "Conditionals",
        ui_endpoint: "/v1/admin/ui/conditionals",
        data_endpoint: "/v1/admin/conditionals",
        icon: "0xe8b5",
    },
    AdminUiPage {
        route: "algos",
        label: "Algo orders",
        ui_endpoint: "/v1/admin/ui/algos",
        data_endpoint: "/v1/admin/algo-orders",
        icon: "0xe626",
    },
];

/// Workload scope grants (self-registration design §3.11). One entry per
/// workload this service presents a certificate for; `vertical enroll`
/// upserts the rows so the fail-closed registry map admits the
/// registration. Scopes use the full vocabulary form:
/// - `exchange8:oms` / `oms` — the service's own workload (both
///   spellings granted; runtime call sites use the bare short name, the
///   map looks up the exact workload): bus produce/consume + the shared
///   certificate identity.
/// - `exchange8:oms-policy` / `oms-policy` — Shield hot-reload
///   consumer + stat emitter pair.
/// - `oms-audit` — shell audit-journal pusher workload (§6.8; boot 8c).
/// - `exchange8-oms-tls` — database-free HostContextResolver resolve workload
///   (TLS-RLS-4: `GET /v1/resolve/hostname`, scope
///   `sentin8:sys:instances:read`).
const M2M_GRANTS: &[M2mGrant] = &[
    M2mGrant {
        workload: "exchange8:oms",
        scopes: &[
            "exchange8:order:read",
            "exchange8:order:write",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "oms",
        scopes: &[
            "exchange8:order:read",
            "exchange8:order:write",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "exchange8:oms-policy",
        scopes: &["sentin8:bus:consume", "sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "oms-policy",
        scopes: &["sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "oms-audit",
        scopes: &["sentin8:sys:audit:write"],
    },
    M2mGrant {
        workload: "exchange8-oms-tls",
        scopes: &["sentin8:sys:instances:read"],
    },
];

const SQL_1_INIT: &str = include_str!("../sql/init.sql");

fn empty_openapi() -> serde_json::Value {
    serde_json::Value::Null
}

pub fn print(section: Option<String>) -> anyhow::Result<()> {
    let reg = Registration {
        service_type: "exchange8:oms",
        subdomain: "exchange8-oms",
        health_path: "/v1/health",
        admin_scopes: ADMIN_SCOPES,
        error_codes: ERROR_CODES,
        consent_providers: &[],
        consent_flows: &[],
        admin_ui: Some(AdminUi {
            label: "OMS",
            icon: "0xe262",
            service_flag: "exchange8-oms",
            pages: ADMIN_UI_PAGES,
        }),
        sql_migrations: vec![SqlMigration {
            version: 1,
            name: "init",
            sql: SQL_1_INIT,
        }],
        pg_roles: PG_ROLES,
        audit_findings: None,
        service_actions: None,
        m2m_grants: Some(M2M_GRANTS),
        openapi_url: "/openapi.json",
        openapi: empty_openapi,
        policies: POLICIES,
        relations: RELATIONS,
        event_triggers: &[],
        schedules: &[],
    };
    sentin8::registration::print_registration(section, &reg, empty_openapi)
}
