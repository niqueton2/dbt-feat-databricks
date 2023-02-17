
with a1 as (
    select * from {{ source('tpch', 'nation') }}
)

select * from a1
