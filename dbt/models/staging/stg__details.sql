{{
    config(
        schema='staging'
    )
}}

with details as (
    SELECT
        {{ dbt_utils.surrogate_key(['PROJECT_URL']) }} as project_key,
        {{ dbt_utils.surrogate_key(['PROJECT_DEVELOPER']) }} as developer_key,
        CAST(PROJECT_FLOORS AS INT) AS Floors,
        PROJECT_FEATURES AS Features,
        PROJECT_YEAR AS Year,
        PROJECT_MAP AS Map
    FROM 
    {{ source('raw', 'json__details') }}
)

SELECT * FROM details