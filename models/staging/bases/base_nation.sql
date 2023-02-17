
with a1 as (
    select * from {{ source('dbt_niqueton', 'nation') }}
)

select * from a1
