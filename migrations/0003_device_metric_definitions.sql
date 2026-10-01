-- Each device has a bounded, persistent set of metric definitions. The configured
-- total definition limit is enforced under the device row lock in the service layer;
-- segment-N definitions may be created atomically by a valid first report.
-- Metric keys are identity; labels may change, while unit and physical meaning must
-- not be reassigned.
CREATE TABLE device_metrics (
    device_id text COLLATE "C" NOT NULL REFERENCES devices(id),
    metric_key text COLLATE "C" NOT NULL,
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 128),
    unit text NOT NULL CHECK (octet_length(unit) BETWEEN 1 AND 32),
    min_value double precision,
    max_value double precision,
    enabled boolean NOT NULL DEFAULT true,
    display_order integer NOT NULL CHECK (display_order > 0),
    PRIMARY KEY (device_id, metric_key),
    CHECK (metric_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    CHECK (metric_key IN ('temperature', 'pressure', 'current') OR metric_key ~ '^segment-([1-9]|[1-9][0-9]|100)$'),
    CHECK (min_value IS NULL OR (min_value > '-Infinity'::double precision AND min_value < 'Infinity'::double precision)),
    CHECK (max_value IS NULL OR (max_value > '-Infinity'::double precision AND max_value < 'Infinity'::double precision)),
    CHECK (min_value IS NULL OR max_value IS NULL OR min_value <= max_value)
);

CREATE TABLE device_metric_latest (
    device_id text COLLATE "C" NOT NULL,
    metric_key text COLLATE "C" NOT NULL,
    value double precision NOT NULL CHECK (value > '-Infinity'::double precision AND value < 'Infinity'::double precision),
    unit text NOT NULL CHECK (octet_length(unit) BETWEEN 1 AND 32),
    sampled_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    message_id text COLLATE "C" NOT NULL,
    PRIMARY KEY (device_id, metric_key),
    FOREIGN KEY (device_id, metric_key) REFERENCES device_metrics(device_id, metric_key)
);

CREATE INDEX device_metrics_display_idx
    ON device_metrics (device_id, display_order, metric_key);

-- The legacy snapshot constraint coupled last_valid_received_at to a whole-message
-- snapshot. Keep the old snapshot internally consistent while making device receipt
-- time independent from the per-metric latest rows.
DO $$
DECLARE
    old_constraint text;
BEGIN
    SELECT conname INTO old_constraint
    FROM pg_constraint
    WHERE conrelid = 'devices'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%latest_sampled_at%'
      AND pg_get_constraintdef(oid) LIKE '%last_valid_received_at%'
    LIMIT 1;
    IF old_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE devices DROP CONSTRAINT %I', old_constraint);
    END IF;
END
$$;

ALTER TABLE devices
    ADD CONSTRAINT devices_legacy_latest_snapshot_check CHECK (
        (latest_sampled_at IS NULL AND latest_received_at IS NULL AND latest_message_id IS NULL AND latest_metrics IS NULL)
        OR
        (latest_sampled_at IS NOT NULL AND latest_received_at IS NOT NULL AND latest_message_id IS NOT NULL AND latest_metrics IS NOT NULL)
    );

-- Existing devices keep their credentials, history, and message IDs. These defaults
-- preserve the three metric keys accepted by the previous protocol implementation.
INSERT INTO device_metrics (device_id, metric_key, display_name, unit, enabled, display_order)
SELECT d.id, defaults.metric_key, defaults.display_name, defaults.unit, true, defaults.display_order
FROM devices AS d
CROSS JOIN (VALUES
    ('temperature', 'temperature', 'C', 1),
    ('pressure', 'pressure', 'kPa', 2),
    ('current', 'current', 'A', 3)
) AS defaults(metric_key, display_name, unit, display_order)
ON CONFLICT (device_id, metric_key) DO NOTHING;

-- Do not prefill segment-1..N rows: only slots present in a valid first report
-- or explicitly configured by the CLI should appear in device queries.

-- The old snapshot belongs to one message. Expand each contained metric into its
-- own state row with the same source times and message ID; never manufacture values.
INSERT INTO device_metric_latest (device_id, metric_key, value, unit, sampled_at, received_at, message_id)
SELECT d.id,
       metric.key,
       (metric.value ->> 'value')::double precision,
       metric.value ->> 'unit',
       d.latest_sampled_at,
       d.latest_received_at,
       d.latest_message_id
FROM devices AS d
CROSS JOIN LATERAL jsonb_each(d.latest_metrics) AS metric(key, value)
WHERE d.latest_metrics IS NOT NULL
  AND d.deleted_at IS NULL
ON CONFLICT (device_id, metric_key) DO NOTHING;

-- Rebuild each metric's latest row from every original history record. The legacy
-- device snapshot contains only one message and can omit a metric whose newest
-- sample was in an earlier partial message. Rank with the same ordering used by
-- telemetry writes: sampled_at, received_at, then bytewise message_id.
WITH ranked_history AS (
    SELECT s.device_id,
           metric.key COLLATE "C" AS metric_key,
           (metric.value ->> 'value')::double precision AS value,
           metric.value ->> 'unit' AS unit,
           s.sampled_at,
           s.received_at,
           s.message_id,
           row_number() OVER (
               PARTITION BY s.device_id, metric.key COLLATE "C"
               ORDER BY s.sampled_at DESC,
                        s.received_at DESC,
                        s.message_id COLLATE "C" DESC
           ) AS metric_rank
    FROM telemetry_samples AS s
    JOIN devices AS d ON d.id = s.device_id AND d.deleted_at IS NULL
    CROSS JOIN LATERAL jsonb_each(s.metrics) AS metric(key, value)
)
INSERT INTO device_metric_latest (device_id, metric_key, value, unit, sampled_at, received_at, message_id)
SELECT device_id, metric_key, value, unit, sampled_at, received_at, message_id
FROM ranked_history
WHERE metric_rank = 1
ON CONFLICT (device_id, metric_key) DO UPDATE SET
    value = EXCLUDED.value,
    unit = EXCLUDED.unit,
    sampled_at = EXCLUDED.sampled_at,
    received_at = EXCLUDED.received_at,
    message_id = EXCLUDED.message_id
WHERE (device_metric_latest.sampled_at, device_metric_latest.received_at, device_metric_latest.message_id)
    < (EXCLUDED.sampled_at, EXCLUDED.received_at, EXCLUDED.message_id);

-- The legacy latest_* snapshot columns are retained as migration/rollback data.
-- Reads and writes after this migration use device_metric_latest instead.
