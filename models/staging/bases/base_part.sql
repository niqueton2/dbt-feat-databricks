with a1 as (
    select * from {{ source('dbt_niqueton', 'part') }}
)

select * from a1