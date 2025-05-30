WITH multi_building_names AS (
    SELECT building_name
    FROM real_estate_metadata
    GROUP BY building_name
    HAVING COUNT(DISTINCT building) > 1
)

SELECT 
    bsm.building,
    JSON_AGG(rem) AS json_agg
FROM buildings_site_mapping bsm
JOIN real_estate_metadata rem
    ON bsm.building = rem.building_name
JOIN multi_building_names mbn
    ON rem.building_name = mbn.building_name
GROUP BY bsm.building
ORDER BY bsm.building ASC;