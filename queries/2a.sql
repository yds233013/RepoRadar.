DROP VIEW IF EXISTS labeled_data;
CREATE VIEW labeled_data AS
WITH medians AS (
    SELECT
        id,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median
    FROM data
    GROUP BY id
),
abs_diffs AS (
    SELECT
        d.id,
        ABS(d.value - m.median) AS abs_diff
    FROM data d
    JOIN medians m ON d.id = m.id
),
mads AS (
    SELECT
        id,
        PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY abs_diff) AS mad
    FROM abs_diffs
    GROUP BY id
)

SELECT
    d.time,
    d.id,
    d.value,
    m.median,
    a.mad,
    CASE
        WHEN a.mad = 0 THEN FALSE
        WHEN ABS(d.value - m.median) > 3 * 1.4826 * a.mad THEN TRUE
        ELSE FALSE
    END AS is_outlier
FROM data d
JOIN medians m ON d.id = m.id
JOIN mads a ON d.id = a.id;

(SELECT * FROM labeled_data WHERE is_outlier ORDER BY time, id LIMIT 50)
UNION ALL
(SELECT * FROM labeled_data WHERE NOT is_outlier ORDER BY time, id LIMIT 50);
