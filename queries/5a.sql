-- BEGIN SOLUTION
SELECT EXISTS (
    SELECT 1
    FROM transitive_subClassOf
    WHERE subject = object
) AS cycle_exists;
-- END SOLUTION
