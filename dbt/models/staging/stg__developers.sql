{{
    config(
        schema='staging'
    )
}}

with developers as (
    select distinct
        {{ dbt_utils.surrogate_key(['PROJECT_DEVELOPER']) }} as developer_key,
        PROJECT_DEVELOPER AS Developer_Name
    from {{ source('raw', 'json__details') }}
    where PROJECT_DEVELOPER is not null
)

select * 
from developers