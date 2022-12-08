{{
    config(
        schema='staging'
    )
}}

with meta_vacancy as (
    select *
    from {{ source('raw', 'json__index_meta') }}
),
detail_vacancy as (
    select *
    from {{ source('raw', 'json__details') }}
),
merged as (
    select 
        {{ dbt_utils.surrogate_key(['m.PROJECT_URL']) }} as project_key,
        CAST(m.project_rent AS INT) index_rent,
        CAST(m.project_sale AS INT) index_sale,
        CAST(d.project_rent AS INT) detail_rent,
        CAST(d.project_sale AS INT) detail_sale
    from meta_vacancy m
    left join detail_vacancy d on m.PROJECT_URL = d.PROJECT_URL 
)

select * 
from merged