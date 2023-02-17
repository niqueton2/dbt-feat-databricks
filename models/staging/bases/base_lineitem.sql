{{
    config(
        materialized='incremental',
        unique_key=["l_orderkey","l_partkey","l_suppkey"]
    )
}}

with a1 as (
    select * from {{ source('dbt_niqueton', 'lineitem') }}
)

select * from a1


{% if is_incremental() %}

where l_load_timestamp >= (select max(l_load_timestamp) from {{ this }})

{% endif %}

