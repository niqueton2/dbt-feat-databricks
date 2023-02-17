with a1 as (
    select * from {{ source('dbt_niqueton', 'region') }}
)

select * from a1