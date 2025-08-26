-- Gold layer: geographic table
-- Adjust column types and joins as needed for your data

{{ config(materialized='table') }}

SELECT
    pl."World_ID",
    pl."World_Name",
    pl."Star_System",
    pl."Planet_Type",
    r."Region_ID",
    r."Full_Name" AS region_name,
    rb."Biome",
    m."Moon_ID",
    m."Moon_Name",
    m."Settlement_Formal",
    m."Colloquial",
    m."Staff_Size",
    m."Specialty"
FROM {{ source('BRONZE', 'planets') }} pl
LEFT JOIN {{ source('BRONZE', 'regions') }} r ON pl."World_ID" = CAST(r."Region_ID" AS VARCHAR)
LEFT JOIN {{ source('BRONZE', 'region_biome') }} rb ON r."Region_ID" = rb."Region_ID"
LEFT JOIN {{ source('BRONZE', 'moons') }} m ON m."Moon_ID" = r."Region_ID"
