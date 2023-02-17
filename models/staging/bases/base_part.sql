with a1 as (
    select * from {{ source('tpch', 'part') }}
)

select * from a1