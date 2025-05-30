-- BEGIN SOLUTION
SELECT NOT EXISTS (
    SELECT subject
    FROM transitive_subClassOf
    GROUP BY subject
    HAVING COUNT(DISTINCT object) > 1
) AS is_tree;
-- END SOLUTION
