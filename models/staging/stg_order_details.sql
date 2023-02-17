{{
    config(
        materialized='incremental',
        unique_key=['l_orderkey','l_suppkey','l_partkey']
    )
}}

with base_orders as (
    select * from {{ ref('base_orders') }}
),

base_lineitem as (
    select * from {{ ref('base_lineitem') }}
),

base_customer as (
    select * from {{ ref('base_customer') }}
)

select 
l.l_ORDERKEY,
l.l_PARTKEY,
l.l_SUPPKEY,
l.l_LINENUMBER,
l.l_RETURNFLAG,
l.l_LINESTATUS,
l.l_SHIPDATE,
l.l_COMMITDATE,
l.l_RECEIPTDATE,
l.l_SHIPINSTRUCT,
l.l_SHIPMODE,
l.l_COMMENT,
l.l_QUANTITY,
l.l_EXTENDEDPRICE,
l.l_DISCOUNT,
l.l_TAX,
o.o_CUSTKEY,
o.o_ORDERSTATUS,
o.o_TOTALPRICE,
o.o_ORDERDATE,
o.o_ORDERPRIORITY,
o.o_SHIPPRIORITY,
o.o_CLERK,
o.o_COMMENT,
c.c_NAME,
c.c_ADDRESS,
c.c_PHONE,
c.c_ACCTBAL,
c.c_MKTSEGMENT,
c.c_COMMENT,
c.c_nationkey

from 
base_lineitem l 
join base_orders o  
on l.l_orderkey=o.o_orderkey
join base_customer c 
on o.o_custkey=c.c_custkey




{% if is_incremental() %}


where l_shipdate >= (select max(l.l_shipdate) from {{ this }})

{% endif %}