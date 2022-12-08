{{
    config(
        schema='staging'
    )
}}

with duplicate_handler as (
    SELECT 
        CODE AS Code,
        NAME AS Name,
        ROUND(LAT, 5) AS Lat,
        ROUND(LONG, 5) AS Long,
        CONCAT(LAT, ',', LONG) AS Coord,
        ROW_NUMBER() OVER(PARTITION BY CODE ORDER BY Name) AS dup_key
    FROM 
    {{ source('raw', 'csv__transit_data') }}
)
, transit_data as (
    SELECT 
        Code,
        Name,
        Lat,
        Long,
        Coord
    FROM duplicate_handler
    WHERE dup_key = 1
)

SELECT
    {{ dbt_utils.surrogate_key(['Code', 'Name'])  }} transit_key,
    * 
FROM transit_data