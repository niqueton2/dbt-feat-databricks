with a1 as (
    select * from {{ source('tpch', 'orders') }}
)

select * from a1