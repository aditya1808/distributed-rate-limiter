CREATE TABLE rules (
    id              text        PRIMARY KEY,
    algorithm       text        NOT NULL CHECK (algorithm IN ('token_bucket', 'sliding_window')),
    limit_count     bigint      NOT NULL CHECK (limit_count > 0),
    period_ms       bigint      NOT NULL CHECK (period_ms > 0),
    burst           bigint      NOT NULL DEFAULT 0 CHECK (burst >= 0),
    burst_window_ms bigint      NOT NULL DEFAULT 0 CHECK (burst_window_ms >= 0),
    fail_open       boolean     NOT NULL DEFAULT false,
    version         bigint      NOT NULL CHECK (version > 0),
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE assignments (
    tenant     text        NOT NULL,
    namespace  text        NOT NULL,
    rule_id    text        NOT NULL REFERENCES rules (id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant, namespace)
);

CREATE INDEX assignments_rule_id_idx ON assignments (rule_id);
CREATE INDEX assignments_namespace_idx ON assignments (namespace);
