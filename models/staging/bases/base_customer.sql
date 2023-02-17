{{
    config(
        materialized='incremental',
        unique_key=["c_custkey"]
    )
}}


with a1 as (
    select * from {{ source('dbt_niqueton', 'customer') }}
)

select * from a1


{% if is_incremental() %}

where c_load_timestamp >= (select max(c_load_timestamp) from {{ this }})

{% endif %}