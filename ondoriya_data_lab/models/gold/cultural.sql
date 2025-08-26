--- Gold layer: cultural table
--- Adjust column types and joins as needed for your data

{{ config(materialized='table') }}

SELECT
    lbb."Language_ID",
    lbb."Language_Name",
    lbb."Branch_From",
    lbb."Phonology_Notes",
    lbb."Morphology_Patterns",
    lbb."Example_Roots",
    lr."Root",
    lr."Meaning",
    lr."Notes",
    m."Moon_ID",
    m."Language_Origin"
FROM {{ source('BRONZE', 'language_building_blocks') }} lbb
LEFT JOIN {{ source('BRONZE', 'language_roots') }} lr ON lbb."Language_Name" = lr."Root"
LEFT JOIN {{ source('BRONZE', 'moons') }} m ON lbb."Language_Name" = m."Language_Origin"
