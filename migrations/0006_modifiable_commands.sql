ALTER TABLE device_metric_latest
    ADD COLUMN modifiable boolean NOT NULL DEFAULT false;

CREATE TABLE device_commands (
    device_id text COLLATE "C" NOT NULL REFERENCES devices(id),
    command_id text COLLATE "C" NOT NULL,
    action text NOT NULL CHECK (action IN ('set_metric', 'clear_override')),
    metric_key text COLLATE "C" NOT NULL CHECK (metric_key ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$'),
    value double precision,
    status text NOT NULL CHECK (status IN ('waiting_to_send', 'broker_acked', 'applied', 'rejected', 'result_unknown', 'cancelled')),
    created_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_attempt_at timestamptz,
    next_attempt_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    broker_acked_at timestamptz,
    result_received_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    CHECK ((action = 'set_metric' AND value IS NOT NULL AND value > '-Infinity'::double precision AND value < 'Infinity'::double precision)
        OR (action = 'clear_override' AND value IS NULL)),
    PRIMARY KEY (device_id, command_id)
);

CREATE FUNCTION keep_device_command_content_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.device_id, NEW.command_id, NEW.action, NEW.metric_key, NEW.value, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.device_id, OLD.command_id, OLD.action, OLD.metric_key, OLD.value, OLD.created_at) THEN
        RAISE EXCEPTION 'device command content is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER device_commands_content_immutable
    BEFORE UPDATE OF device_id, command_id, action, metric_key, value, created_at
    ON device_commands
    FOR EACH ROW EXECUTE FUNCTION keep_device_command_content_immutable();

CREATE INDEX device_commands_due_idx
    ON device_commands (next_attempt_at, created_at)
    WHERE status IN ('waiting_to_send', 'broker_acked');

CREATE UNIQUE INDEX device_commands_one_unresolved_metric_idx
    ON device_commands (device_id, metric_key)
    WHERE status IN ('waiting_to_send', 'broker_acked', 'result_unknown');

CREATE TABLE command_result_anomalies (
    id bigserial PRIMARY KEY,
    topic_device_id text COLLATE "C" NOT NULL,
    command_id text COLLATE "C" NOT NULL,
    reason text NOT NULL,
    payload text NOT NULL,
    received_at timestamptz NOT NULL
);

CREATE INDEX command_result_anomalies_device_time_idx
    ON command_result_anomalies (topic_device_id, received_at DESC);
