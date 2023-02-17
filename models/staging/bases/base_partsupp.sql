with a1 as (
    select * from {{ source('dbt_niqueton', 'partsupp') }}
)

select * from a1