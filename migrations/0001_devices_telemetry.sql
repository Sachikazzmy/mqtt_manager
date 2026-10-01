CREATE TABLE devices (
    id text COLLATE "C" PRIMARY KEY,
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    secret_digest bytea NOT NULL CHECK (octet_length(secret_digest) = 32),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz,
    pending_operation text CHECK (pending_operation IN ('create', 'enable', 'disable', 'reset', 'delete')),
    latest_sampled_at timestamptz,
    latest_received_at timestamptz,
    last_valid_received_at timestamptz,
    latest_message_id text COLLATE "C",
    latest_metrics jsonb,
    CHECK (deleted_at IS NULL OR (enabled = false AND pending_operation IS NULL)),
    CHECK ((latest_sampled_at IS NULL AND latest_received_at IS NULL AND
            last_valid_received_at IS NULL AND latest_message_id IS NULL AND latest_metrics IS NULL)
        OR (latest_sampled_at IS NOT NULL AND latest_received_at IS NOT NULL AND
            last_valid_received_at IS NOT NULL AND latest_message_id IS NOT NULL AND latest_metrics IS NOT NULL))
);

CREATE TABLE telemetry_samples (
    device_id text COLLATE "C" NOT NULL REFERENCES devices(id),
    message_id text COLLATE "C" NOT NULL,
    sampled_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    metrics jsonb NOT NULL,
    PRIMARY KEY (device_id, message_id)
);

CREATE INDEX telemetry_samples_history_idx
    ON telemetry_samples (device_id, sampled_at, received_at, message_id);
CREATE INDEX telemetry_samples_all_history_idx
    ON telemetry_samples (sampled_at, received_at, message_id, device_id);
CREATE INDEX devices_pending_idx ON devices (id) WHERE pending_operation IS NOT NULL;
