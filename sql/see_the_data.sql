-- Read the pipeline's tables from the sandbox's Autonomous Database.
-- Run this in SQL Developer Web (the link is on the sandbox card), as ADMIN.
-- Replace the two values below with your sandbox's bucket and namespace: they
-- are on the card, and inside the database they are also in SBX settings.

define BUCKET    = 'sbx-<your-sandbox>-lake'
define NAMESPACE = '<your object storage namespace>'

-- 1. Credential. The database calls Object Storage as itself, no key stored.
BEGIN
  DBMS_CLOUD_ADMIN.ENABLE_RESOURCE_PRINCIPAL();
END;
/

-- 2. The gold copy the Spark job writes: plain Parquet, always readable.
BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE observations'; EXCEPTION WHEN OTHERS THEN NULL; END;
  DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
    table_name      => 'OBSERVATIONS',
    credential_name => 'OCI$RESOURCE_PRINCIPAL',
    file_uri_list   => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/&NAMESPACE/b/&BUCKET/o/gold/observations/*.parquet',
    format          => JSON_OBJECT('type' VALUE 'parquet'));
END;
/

-- 3. Or read the Iceberg table itself, snapshots and all. Point at the
--    catalog's current metadata file; the Spark job prints its path in the run log.
--
-- BEGIN
--   DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
--     table_name      => 'OBSERVATIONS_ICEBERG',
--     credential_name => 'OCI$RESOURCE_PRINCIPAL',
--     file_uri_list   => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/&NAMESPACE/b/&BUCKET/o/iceberg/db/observations/metadata/vNN.metadata.json',
--     format          => JSON_OBJECT('access_protocol' VALUE JSON_OBJECT('protocol_type' VALUE 'iceberg')));
-- END;
-- /

-- 4. Look at it.
SELECT COUNT(*) readings, COUNT(DISTINCT place) places,
       MIN(observed_at) first_seen, MAX(observed_at) last_seen
  FROM observations;

SELECT place, COUNT(*) readings, ROUND(AVG(temperature_c), 2) avg_value,
       MAX(observed_at) last_seen
  FROM observations
 GROUP BY place
 ORDER BY place;

-- the last hour, newest first
SELECT place, observed_at, condition, temperature_c, humidity_pct, wind_speed_ms
  FROM observations
 WHERE observed_at > SYSTIMESTAMP - INTERVAL '1' HOUR
 ORDER BY observed_at DESC
 FETCH FIRST 50 ROWS ONLY;

-- 5. Ask it in plain English. Select AI is already switched on.
--    SELECT AI which place is warmest right now;
--    SELECT AI how many readings do we have per place today;
