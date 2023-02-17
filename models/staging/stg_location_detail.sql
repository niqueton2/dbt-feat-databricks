with base_region as (
    select * from {{ ref('base_region') }}
),

base_nation as (
    select * from {{ ref('base_nation') }}
)

select 
r_name,
r_comment,
r_regionkey,
n_name,
n_nationkey,
n_comment
from base_region r
inner join base_nation n 
on r.r_regionkey=n.n_regionkey