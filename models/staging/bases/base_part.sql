{{
    config(
        materialized='incremental',
        unique_key=["p_partkey"]
    )
}}


with a1 as (
    select * from {{ source('dbt_niqueton', 'part') }}
)

select * from a1

{% if is_incremental() %}

where p_load_timestamp >= (select max(p_load_timestamp) from {{ this }})

{% endif %}