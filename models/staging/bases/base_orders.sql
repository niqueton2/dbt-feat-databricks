with a1 as (
    select * from {{ source('dbt_niqueton', 'orders') }}
)

select * from a1