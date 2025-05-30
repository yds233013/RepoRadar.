SELECT
    COUNT(DISTINCT units) AS num_unique_units,
    COUNT(DISTINCT LOWER(units)) AS num_unique_units_ignore_case
FROM metadata;