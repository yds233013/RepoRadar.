DROP VIEW IF EXISTS backward;
CREATE VIEW backward AS
WITH annotated AS (
SELECT *,
   
    CASE 
        WHEN value IS NOT NULL THEN value
        ELSE coalesce_agg(next_val) OVER (
            PARTITION BY id, run ORDER BY time DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    END AS run_end,

    
    COUNT(*) OVER (
        PARTITION BY id, run
    ) AS run_size,


    (RANK() OVER (
        PARTITION BY id, run ORDER BY time
    )) - 1 AS run_rank
    FROM forward
)

SELECT 
    id, time, value, run, run_start, next_val, run_end, run_rank, run_size
FROM annotated;


SELECT * FROM backward WHERE run_size > 2 ORDER BY id, run, run_rank LIMIT 100;

