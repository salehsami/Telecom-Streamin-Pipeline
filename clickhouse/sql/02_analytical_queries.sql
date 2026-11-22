
-- ============================================================
-- TELECOM FRAUD & USAGE INTELLIGENCE
-- Analytical SQL queries for ClickHouse
-- ============================================================

-- 1. FRAUD: Alerts by rule
-- Which fraud detection rules trigger most often?

SELECT
    rule_name,
    count() AS total_records,
    uniqExact(alert_id) AS unique_alerts
FROM telecom.fraud_alerts
GROUP BY rule_name
ORDER BY total_records DESC;


-- 2. FRAUD: Alerts by severity
-- How serious are the detected incidents?

SELECT
    severity,
    count() AS total_records,
    uniqExact(alert_id) AS unique_alerts
FROM telecom.fraud_alerts
GROUP BY severity
ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
        ELSE 5
    END;


-- 3. FRAUD: Customers with the most alerts
-- Which customers appear most frequently?

SELECT
    customer_id,
    count() AS total_records,
    uniqExact(alert_id) AS unique_alerts,
    countIf(severity = 'CRITICAL') AS critical_records,
    countIf(severity = 'HIGH') AS high_records
FROM telecom.fraud_alerts
GROUP BY customer_id
ORDER BY total_records DESC
LIMIT 10;


-- 4. FRAUD: Recent critical incidents
-- Show the latest critical alerts for investigation.

SELECT
    alert_id,
    customer_id,
    rule_name,
    country_code,
    detected_at,
    evidence
FROM telecom.fraud_alerts
WHERE severity = 'CRITICAL'
ORDER BY detected_at DESC
LIMIT 20;


-- 5. FRAUD: Alert trend by hour
-- How does fraud detection activity change over time?

SELECT
    toStartOfHour(detected_at) AS hour,
    rule_name,
    count() AS total_records,
    uniqExact(alert_id) AS unique_alerts
FROM telecom.fraud_alerts
WHERE detected_at >= now() - INTERVAL 7 DAY
GROUP BY hour, rule_name
ORDER BY hour;


-- 6. USAGE: Total data consumption by country

SELECT
    country_code,
    round(sum(total_megabytes), 2) AS total_megabytes,
    round(sum(total_megabytes) / 1024, 2) AS total_gigabytes,
    sum(event_count) AS total_events
FROM telecom.usage_aggregates
GROUP BY country_code
ORDER BY total_megabytes DESC;


-- 7. USAGE: Consumption over time
-- One-minute usage trend for each country.

SELECT
    window_start,
    country_code,
    total_megabytes,
    event_count
FROM telecom.usage_aggregates
WHERE window_start >= now() - INTERVAL 24 HOUR
ORDER BY window_start;


-- 8. USAGE: Highest-consumption minute windows

SELECT
    country_code,
    window_start,
    total_megabytes,
    event_count
FROM telecom.usage_aggregates
ORDER BY total_megabytes DESC
LIMIT 10;
