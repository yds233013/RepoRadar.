DROP VIEW IF EXISTS likely_data;
CREATE VIEW likely_data AS
SELECT
    id,
    time,
    value,
    run,
    run_start,
    next_val,
    run_end,
    run_rank,
    run_size,
    CASE
        WHEN value IS NULL THEN run_start + (run_rank::FLOAT * (run_end - run_start) / run_size)
        ELSE value
    END AS interpolated
FROM backward;

SELECT * FROM likely_data WHERE run_size > 2 ORDER BY id, time LIMIT 100;