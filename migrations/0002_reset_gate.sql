ALTER TABLE devices
    ADD COLUMN requires_secret_reset boolean NOT NULL DEFAULT false;

ALTER TABLE devices
    ADD CONSTRAINT devices_reset_gate CHECK (NOT requires_secret_reset OR NOT enabled);
