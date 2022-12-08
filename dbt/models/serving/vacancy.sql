{{
    config(
        schema='serving'
    )
}}

with vacancy as (
    select *
    from {{ ref('stg__vacancy') }}
),
list as (
    select *
    from {{ ref('stg__projects_list') }}
),
merged as (
    select 
        l.project_key,
        l.name,
        v.index_rent,
        v.index_sale,
        v.detail_rent,
        v.detail_sale
    from list l
    left join vacancy v on l.project_key = v.project_key 
)

select * 
from merged