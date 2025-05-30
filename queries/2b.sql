DROP VIEW IF EXISTS cleaned_data;
CREATE VIEW cleaned_data AS
SELECT *,
    CASE
        WHEN mad = 0 THEN value
        WHEN is_outlier = FALSE THEN value
        WHEN value > median THEN median + 3 * 1.4826 * mad
        ELSE median - 3 * 1.4826 * mad
    END AS clean_value
FROM labeled_data;

(SELECT * FROM cleaned_data WHERE is_outlier ORDER BY time, id LIMIT 50)
UNION ALL
(SELECT * FROM cleaned_data WHERE NOT is_outlier ORDER BY time, id LIMIT 50);
