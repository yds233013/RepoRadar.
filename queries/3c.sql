WITH unit_counts AS (
    SELECT class, COUNT(DISTINCT units) AS unit_count
    FROM metadata
    GROUP BY class
)
SELECT BOOL_AND(unit_count = 1) AS are_units_consistent
FROM unit_counts;