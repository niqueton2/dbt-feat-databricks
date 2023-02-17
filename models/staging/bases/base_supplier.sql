{{
    config(
        materialized='incremental',
        unique_key=["s_suppkey"]
    )
}}





with a1 as (
    select * from {{ source('dbt_niqueton', 'supplier') }}
)

select * from a1


{% if is_incremental() %}

where s_load_timestamp >= (select max(s_load_timestamp) from {{ this }})

{% endif %}