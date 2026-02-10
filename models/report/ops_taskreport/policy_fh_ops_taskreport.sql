{{		config (			
        materialized="view",			
        alias='policy', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

SELECT
    *
FROM
    {{ ref('policy_vc_clean_insurance') }}