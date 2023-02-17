with a1 as (
    select * from {{ source('dbt_niqueton', 'customer') }}
)

select * from a1