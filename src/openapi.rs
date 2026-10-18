// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Oms
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI spec for Exchange8:Oms.
//!
//! utoipa annotations on the service's handlers + the shared Shield
//! policy-admin surface (`sentin8::shell::policy_*` wrappers); the
//! shell-owned `/v1/health` mount is described through a spec wrapper
//! (`sentin8/design/sentin8-openapi-coverage-design.md` §2).

use utoipa::OpenApi;

pub mod w {

    /// Spec wrapper for the shell-owned `/v1/health` mount.
    #[utoipa::path(
        get,
        path = "/v1/health",
        responses(
            (status = 200, description = "Service is healthy", body = serde_json::Value),
        ),
        security(),
        tag = "oms",
    )]
    pub async fn oms_health() {}
}

/// OpenAPI spec for the oms API.
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::runtime::admin_ui_page,
        crate::runtime::admin_pegs,
        crate::runtime::admin_conditionals,
        crate::runtime::admin_algo_orders,
        crate::runtime::pause_algo_order,
        crate::runtime::resume_algo_order,
crate::openapi::w::oms_health,
        sentin8::shell::policy_list_sets,
        sentin8::shell::policy_get_set,
        sentin8::shell::policy_put_set,
        sentin8::shell::policy_delete_set,
        sentin8::shell::policy_test_set,
        sentin8::shell::policy_mode_enforce,
        sentin8::shell::policy_mode_shadow,
        sentin8::shell::policy_mode_disable,
        sentin8::shell::policy_create_set,
        sentin8::shell::policy_save_set,
        sentin8::shell::policy_flip_set,
        sentin8::shell::policy_list_decisions,
        sentin8::shell::policy_delete_relation_path,
        sentin8::shell::policy_delete_attribute_path,
        sentin8::shell::policy_list_relations,
        sentin8::shell::policy_put_relation,
        sentin8::shell::policy_delete_relation,
        sentin8::shell::policy_check_relation,
        sentin8::shell::policy_list_attributes,
        sentin8::shell::policy_put_attribute,
        sentin8::shell::policy_delete_attribute,
    ),
    components(schemas()),
    security(),
    tags(
        (name = "oms", description = "Exchange8:Oms"),
        (name = "exchange8-admin", description = "Exchange8 admin surface (RFW)"),
        (name = "policy", description = "Shield policy administration (shared surface)"),
    ),
)]
pub struct OmsOpenApi;
