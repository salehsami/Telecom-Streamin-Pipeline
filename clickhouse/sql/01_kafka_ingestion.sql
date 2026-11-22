
-- ============================================================
-- TELECOM ANALYTICS DATABASE
-- Kafka -> ClickHouse -> MergeTree
-- ============================================================

CREATE DATABASE IF NOT EXISTS telecom;

-- ============================================================
-- 1. FRAUD ALERTS: DURABLE ANALYTICAL TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS telecom.fraud_alerts
(
    alert_id String,
    rule_name LowCardinality(String),
    rule_version UInt16,
    severity LowCardinality(String),

    customer_id String,
    country_code LowCardinality(String),

    detected_at DateTime64(3, 'UTC'),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),

    contributing_event_ids Array(String),
    evidence String,

    -- Optional fields vary between fraud rules.
    attempt_count Nullable(UInt32),
    top_up_amount Nullable(Float64),
    currency Nullable(String),
    new_device_id Nullable(String),

    -- Preserve the original event, including future fields.
    raw_json String,

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, rule_name, alert_id);


-- ============================================================
-- 2. USAGE AGGREGATES: DURABLE ANALYTICAL TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS telecom.usage_aggregates
(
    country_code LowCardinality(String),

    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),

    total_megabytes Float64,
    event_count UInt64,

    contributing_event_ids Array(String),

    raw_json String,

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, country_code);


-- ============================================================
-- 3. KAFKA SOURCE: FRAUD ALERTS
-- ============================================================

CREATE TABLE IF NOT EXISTS telecom.fraud_alerts_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'telecom.fraud.alerts.v1',
    kafka_group_name = 'clickhouse-fraud-alerts-v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;


-- ============================================================
-- 4. KAFKA SOURCE: USAGE AGGREGATES
-- ============================================================

CREATE TABLE IF NOT EXISTS telecom.usage_aggregates_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'telecom.usage.country.minute.v1',
    kafka_group_name = 'clickhouse-usage-aggregates-v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;


-- ============================================================
-- 5. FRAUD ALERT MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.fraud_alerts_mv
TO telecom.fraud_alerts
AS
SELECT
    JSONExtractString(raw, 'alert_id') AS alert_id,

    JSONExtractString(raw, 'rule_name') AS rule_name,

    toUInt16(JSONExtractUInt(raw, 'rule_version')) AS rule_version,

    JSONExtractString(raw, 'severity') AS severity,

    JSONExtractString(raw, 'customer_id') AS customer_id,

    JSONExtractString(raw, 'country_code') AS country_code,

    parseDateTime64BestEffort(
        JSONExtractString(raw, 'detected_at'), 3, 'UTC'
    ) AS detected_at,

    parseDateTime64BestEffort(
        JSONExtractString(raw, 'window_start'), 3, 'UTC'
    ) AS window_start,

    parseDateTime64BestEffort(
        JSONExtractString(raw, 'window_end'), 3, 'UTC'
    ) AS window_end,

    JSONExtract(raw, 'contributing_event_ids', 'Array(String)')
        AS contributing_event_ids,

    JSONExtractString(raw, 'evidence') AS evidence,

    if(
        JSONHas(raw, 'attempt_count'),
        toUInt32(JSONExtractUInt(raw, 'attempt_count')),
        NULL
    ) AS attempt_count,

    if(
        JSONHas(raw, 'top_up_amount'),
        JSONExtractFloat(raw, 'top_up_amount'),
        NULL
    ) AS top_up_amount,

    if(
        JSONHas(raw, 'currency'),
        JSONExtractString(raw, 'currency'),
        NULL
    ) AS currency,

    if(
        JSONHas(raw, 'new_device_id'),
        JSONExtractString(raw, 'new_device_id'),
        NULL
    ) AS new_device_id,

    raw AS raw_json

FROM telecom.fraud_alerts_queue;


-- ============================================================
-- 6. USAGE AGGREGATE MATERIALIZED VIEW
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS telecom.usage_aggregates_mv
TO telecom.usage_aggregates
AS
SELECT
    JSONExtractString(raw, 'country_code') AS country_code,

    parseDateTime64BestEffort(
        JSONExtractString(raw, 'window_start'), 3, 'UTC'
    ) AS window_start,

    parseDateTime64BestEffort(
        JSONExtractString(raw, 'window_end'), 3, 'UTC'
    ) AS window_end,

    JSONExtractFloat(raw, 'total_megabytes') AS total_megabytes,

    JSONExtractUInt(raw, 'event_count') AS event_count,

    JSONExtract(raw, 'contributing_event_ids', 'Array(String)')
        AS contributing_event_ids,

    raw AS raw_json

FROM telecom.usage_aggregates_queue;
