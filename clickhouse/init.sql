CREATE DATABASE IF NOT EXISTS defects_db;

CREATE TABLE IF NOT EXISTS defects_db.defects_agg (
    turbine_id String,
    defect_type String,
    total_events UInt32,
    avg_criticality Float32,
    max_temp_delta Float32,
    dt Date
) ENGINE = MergeTree()
ORDER BY (turbine_id, dt);