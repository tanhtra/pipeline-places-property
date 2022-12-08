{{
    config(
        schema='staging'
    )
}}

with places_data as (
    SELECT DISTINCT
        NAME AS Name,
        "geocodes.main.latitude" AS Lat,
        "geocodes.main.longitude" AS Long,
        "location.formatted_address" AS Address,
        CONCAT("geocodes.main.latitude", ',', "geocodes.main.longitude") AS Coord
    FROM 
    {{ source('raw', 'csv__places_data_scd') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['Name', 'Address'])  }} places_key,
    * 
FROM places_data
