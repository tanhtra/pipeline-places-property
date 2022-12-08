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
        v.index_rent,
        v.index_sale,
        v.detail_rent,
        v.detail_sale,
        d.Developer_Name
    from list l
    left join details on l.project_key = details.project_key
    left join vacancy v on l.project_key = v.project_key 
    left join developers d on details.developer_key = d.developer_key 
),
processing as (
    SELECT 
        Developer_Name,
        SUM(index_rent) AS total_index_rent,
        SUM(index_sale) AS total_index_sale,
        SUM(detail_rent) AS total_detail_rent,
        SUM(detail_sale) AS total_detail_sale
    FROM merged
    WHERE Developer_Name IS NOT NULL
    GROUP BY Developer_Name
    ORDER BY Developer_Name
)

select *
from processing