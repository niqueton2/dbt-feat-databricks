
{{
    config(
        materialized='incremental',
        unique_key=['ps_suppkey','ps_partkey']
    )
}}

with base_part as (
    select * from {{ ref('base_part') }}
),

base_partsupp as (
    select * from {{ ref('base_partsupp') }}
),

base_supplier as (
    select * from {{ ref('base_supplier') }}
)

select 

s.s_NAME,
s.s_ADDRESS,
s.s_NATIONKEY,
s.s_PHONE,
s.s_ACCTBAL,
s.s_COMMENT,
p.p_NAME,
p.p_MFGR,
p.p_BRAND,
p.p_TYPE,
p.p_SIZE,
p.p_CONTAINER,
p.p_COMMENT,
p.p_RETAILPRICE,
ps.ps_PARTKEY,
ps.ps_SUPPKEY,
ps.ps_AVAILQTY,
ps.ps_SUPPLYCOST,
ps.ps_COMMENT,
ps.ps_load_timestamp

from base_supplier s 
join base_partsupp ps 
on s.s_suppkey=ps.ps_suppkey
join base_part p 
on ps.ps_partkey=p.p_partkey 


{% if is_incremental() %}

where ps.ps_load_timestamp >= (select max(ps_load_timestamp) from {{ this }})

{% endif %}