CREATE OR REPLACE VIEW complete AS
SELECT id, time, value
FROM data

UNION ALL

SELECT
    g.id,
    gs.generated_time,
    NULL AS value
FROM gaps g,
LATERAL GENERATE_SERIES(
    g.lag_time + INTERVAL '15 minutes',
    g.time - INTERVAL '15 minutes',
    INTERVAL '15 minutes'
) AS gs(generated_time);

-- This is needed to return the correct full dataset for autograding
SELECT * FROM complete ORDER BY id, time LIMIT 100;