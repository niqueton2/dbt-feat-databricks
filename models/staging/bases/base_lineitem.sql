{{
    config(
        materialized='table'
    )
}}

with a1 as (
    select * from {{ source('dbt_niqueton', 'lineitem') }}
)

select * from a1