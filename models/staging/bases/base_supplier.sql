with a1 as (
    select * from {{ source('dbt_niqueton', 'supplier') }}
)

select * from a1