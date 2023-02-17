{{
    config(
        materialized='table'
    )
}}

with a1 as (
    select * from {{ source('tpch', 'lineitem') }}
)

select * from a1