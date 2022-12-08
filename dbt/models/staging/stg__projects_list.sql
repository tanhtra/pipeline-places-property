{{
    config(
        schema='staging'
    )
}}

with index_meta as (
    SELECT
        {{ dbt_utils.surrogate_key(['PROJECT_URL']) }} as project_key,
        PROJECT_NAME AS Name,
        PROJECT_URL AS URL
    FROM 
    {{ source('raw', 'json__index_meta') }}
)

SELECT * FROM index_meta