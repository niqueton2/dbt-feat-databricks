with a1 as (
    select * from {{ source('tpch', 'region') }}
)

select * from a1