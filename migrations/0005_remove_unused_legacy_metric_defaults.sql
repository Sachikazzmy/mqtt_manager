-- New devices no longer receive legacy placeholder definitions. Remove only
-- unused defaults from existing devices; definitions that back Latest or history
-- remain so stored measurements and their protocol binding stay intact.
DELETE FROM device_metrics AS definition
WHERE definition.metric_key IN ('temperature', 'pressure', 'current')
  AND NOT EXISTS (
      SELECT 1
      FROM device_metric_latest AS latest
      WHERE latest.device_id = definition.device_id
        AND latest.metric_key = definition.metric_key
  )
  AND NOT EXISTS (
      SELECT 1
      FROM telemetry_samples AS sample
      WHERE sample.device_id = definition.device_id
        AND sample.metrics ? definition.metric_key
  );
