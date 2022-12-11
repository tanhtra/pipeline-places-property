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
    select *
    from {{ ref('stg__vacancy') }}
),
merged as (
    select 
        l.name,
        l.url,
        v.detail_rent,
        v.detail_sale,
        d.floors,
        d.features,
        d.year,
        d.map,
        dev.Developer_Name
    from
        list l
        left join details d on l.project_key = d.project_key
        left join developers dev on d.developer_key = dev.developer_key
        left join vacancy v on l.project_key = v.project_key
)

select * 
from merged