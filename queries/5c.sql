-- BEGIN STARTER CODE
-- WITH sensor_children AS (
--     SELECT ______
--     FROM ______
--     WHERE object = ______
--         AND predicate = ______
-- )
-- SELECT ______, ______
-- FROM metadata AS md
-- INNER JOIN mapping AS map ON ______
-- INNER JOIN transitive_subClassOf AS t ON ______
-- INNER JOIN sensor_children AS sc ON ______
-- GROUP BY ______
-- ORDER BY ______;
-- END STARTER CODE

-- BEGIN SOLUTION 
WITH sensor_children AS (
    SELECT subject AS sensor_child
    FROM ontology
    WHERE object = 'https://brickschema.org/schema/Brick#Sensor'
      AND predicate = 'http://www.w3.org/2000/01/rdf-schema#subClassOf'
),
descendant_to_child AS (
    SELECT t.subject AS descendant, sc.sensor_child
    FROM transitive_subClassOf t
    JOIN sensor_children sc ON t.object = sc.sensor_child
),
metadata_mapped AS (
    SELECT md.id, mp.brickclass
    FROM metadata md
    JOIN mapping mp ON md.class = mp.rawname
),
unique_sensor_ids AS (
    SELECT DISTINCT dc.sensor_child, md.id
    FROM descendant_to_child dc
    JOIN metadata_mapped md ON dc.descendant = md.brickclass
)
SELECT sensor_child, COUNT(*) AS count
FROM unique_sensor_ids
GROUP BY sensor_child
ORDER BY sensor_child;
-- END SOLUTION
