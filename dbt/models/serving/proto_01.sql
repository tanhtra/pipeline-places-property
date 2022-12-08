{{
    config(
        schema='serving'
    )
}}
with list as (
    select *
    from {{ ref('stg__projects_list') }}
),
details as (
    select * 
    from {{ ref('stg__details') }}
),
developers as (
    select *
    from {{ ref('stg__developers') }}
),
vacancy as (
    select * from {{ ref('vacancy') }}
),
merged as (
    select 
        l.name,
        l.url,
        details.floors,
        details.features,
        details.year,
        details.map,
        v.index_rent,
        v.index_sale,
        v.detail_rent,
        v.detail_sale,
        d.Developer_Name
    from list l
    left join details on l.project_key = details.project_key
    left join vacancy v on l.project_key = v.project_key 
    left join developers d on details.developer_key = d.developer_key 
)

select *
from merged