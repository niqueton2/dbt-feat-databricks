{{
    config(
        materialized='incremental',
        unique_key=["o_orderkey"]
    )
}}


with a1 as (
    select * from {{ source('dbt_niqueton', 'orders') }}
)

select * from a1

{% if is_incremental() %}

where o_load_timestamp > 
 (
    select
     max(o_load_timestamp) 
     from {{ this }}
 )
)

{% endif %}