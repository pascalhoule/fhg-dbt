{{		config (			
        materialized="view",			
        alias='policy', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	


with p as (
    select *
    from {{ ref('policy_vc_clean_insurance') }}
),
s as (
    select
        statusvalue,
        englishdescription as policy_status
    from {{ ref('policystatus_vc_clean_insurance') }}
)

select
    p.*,
    s.policy_status
from p
left join s
    on p.status = s.statusvalue
