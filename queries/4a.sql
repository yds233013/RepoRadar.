DROP VIEW IF EXISTS gaps;
CREATE VIEW gaps AS
WITH with_lags AS (
    SELECT
        id,
        DATE_TRUNC('minute', time) AS time,
        value,
        DATE_TRUNC('minute', LAG(time) OVER (PARTITION BY id ORDER BY time)) AS lag_time,
        LAG(value) OVER (PARTITION BY id ORDER BY time) AS lag_value,
        DATE_TRUNC('minute', time) - DATE_TRUNC('minute', LAG(time) OVER (PARTITION BY id ORDER BY time)) AS time_diff
    FROM data
)
SELECT *
FROM with_lags
WHERE time_diff >= INTERVAL '30 minutes';
SELECT * FROM gaps ORDER BY id, time;
