SELECT
    rem.building_name,
    rem.address,
    rem.location,
    (
        SELECT loc_name
        FROM uc_locations ucl
        ORDER BY word_similarity(rem.location, ucl.loc_name) DESC
        LIMIT 1
    ) AS clean_location
FROM real_estate_metadata rem;