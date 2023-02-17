with a1 as (
    select * from {{ source('tpch', 'customer') }}
)

select * from a1