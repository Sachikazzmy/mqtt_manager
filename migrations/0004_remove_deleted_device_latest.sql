-- A previous version of 0003 could rebuild Latest for soft-deleted devices from
-- their retained history. Keep tombstones consistent with the delete operation.
DELETE FROM device_metric_latest AS latest
USING devices AS device
WHERE latest.device_id = device.id
  AND device.deleted_at IS NOT NULL;
