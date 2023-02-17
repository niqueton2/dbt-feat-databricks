
{{
    config(
        materialized='incremental',
        unique_key=["ps_partkey","ps_suppkey"]
    )
}}

with a1 as (
    select * from {{ source('dbt_niqueton', 'partsupp') }}
)

select * from a1


{% if is_incremental() %}

where ps_load_timestamp >= (select max(ps_load_timestamp) from {{ this }})

{% endif %}