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
merged as (
    select 
        l.project_key,
        l.name,
        l.url,
        d.floors,
        d.features,
        d.year,
        d.map,
        dev.Developer_Name
    from
        list l
        left join details d on l.project_key = d.project_key
        left join developers dev on d.developer_key = dev.developer_key
)

select * 
from merged