with a1 as (
    select * from {{ source('tpch', 'partsupp') }}
)

select * from a1