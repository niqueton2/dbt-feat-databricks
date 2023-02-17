{{
    config(
        materialized='incremental',
        unique_key=['l_orderkey','l_suppkey','l_partkey']
    )
}}




with stg_od as (
    select * from {{ ref('stg_order_details') }}
),

stg_sd as (
    select * from {{ ref('stg_supplier_detail') }}
),

stg_ld as (
    select * from {{ ref('stg_location_detail') }}
),

aux1 as (


select

l.n_name ,
l.r_name ,
o.l_PARTKEY,
o.l_SUPPKEY,
o.l_QUANTITY,
o.l_EXTENDEDPRICE,
o.l_DISCOUNT,
o.l_TAX,
o.o_TOTALPRICE,
o.o_ORDERDATE,
o.c_NAME,
o.l_load_timestamp

from stg_ld l 
join stg_od o 
on o.c_nationkey=l.n_nationkey
),

aux2 as (

select

s.ps_suppkey,
s.ps_partkey,
s.s_NAME,
s.ps_SUPPLYCOST,
s.p_BRAND,
s.p_RETAILPRICE,
s.p_SIZE,
s.p_TYPE,
l.n_name,
l.r_name,
s.ps_load_timestamp

from stg_ld l 
join stg_sd s 
on s.s_NATIONKEY=l.n_nationkey
    
)


select 

u.n_name  as customer_nation,
u.r_name  as customer_region,
u.l_PARTKEY,
u.l_SUPPKEY,
u.l_QUANTITY,
u.l_EXTENDEDPRICE,
u.l_DISCOUNT,
u.l_TAX,
u.o_TOTALPRICE,
u.o_ORDERDATE,
u.c_NAME,
u.l_load_timestamp,
d.ps_suppkey,
d.ps_partkey,
d.s_NAME,
d.ps_SUPPLYCOST,
d.p_BRAND,
d.p_RETAILPRICE,
d.p_SIZE,
d.p_TYPE,
d.n_name  as supplier_nation,
d.r_name as supplier_region,
d.ps_load_timestamp

from aux1 u
join aux2 as d
on
(d.ps_PARTKEY=u.l_partkey and d.ps_suppkey=u.l_suppkey)




{% if is_incremental() %}


where 
(
    d.ps_load_timestamp >= (select max(ps_load_timestamp) from {{ this }})
    or
    u.l_load_timestamp >= (select max(l_load_timestamp) from {{ this }})
)

{% endif %}


