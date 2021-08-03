with conn_dsnp as (

select *

from
{{ ref('scores_conn_dsnp_mid_final') }}
),


conn_medicare as (

select *

from
{{ ref('scores_conn_medicare_mid_final') }}
),


emblem as (

select *

from
{{ ref('scores_emblem_mid_final') }}
),


tufts as (

select *

from
{{ ref('scores_tufts_mid_final') }}
),

joinedscores as (

select * from tufts
union all
select * from emblem
union all
select * from conn_medicare
union all
select * from conn_dsnp
)

select * from joinedscores