WITH multi_mapped_names AS (
    SELECT building_name
    FROM real_estate_metadata
    JOIN buildings_site_mapping
        ON real_estate_metadata.building_name = buildings_site_mapping.building
    GROUP BY building_name
    HAVING COUNT(DISTINCT buildings_site_mapping.site) > 1
)

SELECT 
    rem.building_name,
    JSON_AGG(bsm) AS json_agg
FROM multi_mapped_names mmn
JOIN real_estate_metadata rem
    ON rem.building_name = mmn.building_name
JOIN buildings_site_mapping bsm
    ON rem.building_name = bsm.building
GROUP BY rem.building_name
ORDER BY rem.building_name ASC;