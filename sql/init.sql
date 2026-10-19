-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:OMS
-- SPDX-License-Identifier: Apache-2.0

-- Ex8:OMS - conditional (stop) orders, complex orders, algo parents.
--
-- Design: design/sentin8-exchange8-data-model-technical-design.md §3.11 (OMS).
-- No migration code (new platform): `sentin8-cli vertical init` applies this.

CREATE SCHEMA IF NOT EXISTS oms;

-- Stop/conditional/OCO state.
CREATE TABLE IF NOT EXISTS oms.conditional_order (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    order_id        BIGINT NOT NULL,
    condition_type  TEXT NOT NULL CHECK (condition_type IN
                    ('stop_last','stop_bid','stop_ask','stop_index',
                     'trailing_stop','oca','oco')),
    trigger_price   NUMERIC(18,8),
    trail_bps       NUMERIC(10,4),
    triggered       BOOLEAN NOT NULL DEFAULT FALSE,
    trigger_ts      TIMESTAMPTZ,
    oco_group       UUID,                -- OCO/ OCA group id
    armed           BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_exchange8_condord_env ON oms.conditional_order (env_id);

-- Complex/multi-leg orders (options strategies etc.).
CREATE TABLE IF NOT EXISTS oms.complex_order (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    order_id    BIGINT NOT NULL,
    strategy    TEXT NOT NULL,           -- e.g. calendar, vertical, combo
    limit_price NUMERIC(18,8),
    UNIQUE (env_id, order_id)
);
CREATE TABLE IF NOT EXISTS oms.complex_order_leg (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    complex_order_id UUID NOT NULL REFERENCES oms.complex_order(id),
    env_id          UUID NOT NULL,
    security_id     UUID,
    leg_ratio       NUMERIC(10,4) NOT NULL DEFAULT 1,
    leg_action      CHAR(1) NOT NULL CHECK (leg_action IN ('B','S'))
);

-- Algo parent orders (TWAP/VWAP/Iceberg-parent etc.).
CREATE TABLE IF NOT EXISTS oms.algo_order (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    parent_order_id BIGINT NOT NULL,
    algo_type   TEXT NOT NULL CHECK (algo_type IN ('TWAP','VWAP','POV','Custom')),
    slice_size  NUMERIC(18,8),
    start_ts    TIMESTAMPTZ,
    end_ts      TIMESTAMPTZ,
    state       TEXT NOT NULL DEFAULT 'running'
                CHECK (state IN ('running','paused','complete','cancelled')),
    UNIQUE (env_id, parent_order_id)
);
CREATE TABLE IF NOT EXISTS oms.algo_child_order (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    algo_order_id UUID NOT NULL REFERENCES oms.algo_order(id),
    child_order_id BIGINT NOT NULL,
    slice_no    INTEGER NOT NULL,
    UNIQUE (env_id, algo_order_id, slice_no)
);

-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant tables.
-- Policies use the platform helpers sentin8_context_initialized() and
-- sentin8_current_env_id(); services call set_tenant_context on the same
-- connection as the data query.
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['oms.conditional_order', 'oms.complex_order', 'oms.complex_order_leg', 'oms.algo_order', 'oms.algo_child_order'] LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
            EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_select ON %2$s;
CREATE POLICY %1$I_select ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_insert ON %2$s;
CREATE POLICY %1$I_insert ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_update ON %2$s;
CREATE POLICY %1$I_update ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_delete ON %2$s;
CREATE POLICY %1$I_delete ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Per-service role grants (schema-per-service: this service owns schema
-- `oms`; grants live in db `exchange8` only - design doc §1).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_oms_service') THEN
        ALTER ROLE exchange8_oms_service SET search_path = oms, public;
        GRANT USAGE ON SCHEMA oms TO exchange8_oms_service;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA oms TO exchange8_oms_service;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA oms TO exchange8_oms_service;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox (DBX §6.7 - generated from `sentin8_outbox::outbox_table_sql(
-- "oms")`): rows written in the same DB transaction as the domain change;
-- the shell's relay (boot step §8b) pushes them to Bus `POST /v1/produce`.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS oms.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_oms_outbox_unrelayed
    ON oms.outbox (created_at) WHERE relayed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE oms.outbox ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE oms.outbox FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS outbox_select ON oms.outbox';
    EXECUTE 'CREATE POLICY outbox_select ON oms.outbox FOR SELECT
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_insert ON oms.outbox';
    EXECUTE 'CREATE POLICY outbox_insert ON oms.outbox FOR INSERT
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_update ON oms.outbox';
    EXECUTE 'CREATE POLICY outbox_update ON oms.outbox FOR UPDATE
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for oms.outbox: %', SQLERRM;
END $$;

CREATE OR REPLACE FUNCTION oms.oms_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('oms_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON oms.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON oms.outbox
    FOR EACH ROW
    EXECUTE FUNCTION oms.oms_notify_outbox();

-- ---------------------------------------------------------------------------
-- Local audit journal (DBX §6.8 - generated from
-- `sentin8_audit::audit_journal_sql("oms")`): rows written in the same
-- transaction as the domain change; the shell's pusher (boot step §8c) pushes
-- them to Sys `POST /v1/audit` (`sentin8:sys:audit:write`). A Sys outage
-- never drops audit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS oms.audit_journal (
    id              UUID PRIMARY KEY,
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_oms_audit_journal_unpushed
    ON oms.audit_journal (created_at) WHERE pushed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE oms.audit_journal ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE oms.audit_journal FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS audit_journal_dml ON oms.audit_journal';
    EXECUTE 'CREATE POLICY audit_journal_dml ON oms.audit_journal
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for oms.audit_journal: %', SQLERRM;
END $$;

-- Outbox + journal role grants (same pattern as the table grants above).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_oms_service') THEN
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA oms TO exchange8_oms_service;
    END IF;
END $$;
