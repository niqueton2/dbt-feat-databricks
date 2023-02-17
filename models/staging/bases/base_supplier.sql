with a1 as (
    select * from {{ source('tpch', 'supplier') }}
)

select * from a1