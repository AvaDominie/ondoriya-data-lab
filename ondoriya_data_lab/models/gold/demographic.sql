-- Gold layer: demographic table
-- Adjust column types and joins as needed for your data

{{ config(materialized='table') }}

SELECT
    f."Faction",
    f."Regions",
    f."Percent",
    h."household_id",
    h."household_type",
    p."person_id",
    p."first_name",
    p."age",
    p."language",
    p."current_region_id",
    p."family_name"
FROM {{ source('BRONZE', 'faction_distribution') }} f
LEFT JOIN {{ source('BRONZE', 'households') }} h ON f."Regions" = h."region_id"
LEFT JOIN {{ source('BRONZE', 'people') }} p ON h."household_id" = p."household_id"
