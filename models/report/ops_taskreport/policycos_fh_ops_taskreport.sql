{{		config (			
        materialized="view",			
        alias='policycos', 			
        database='report', 			
        schema='ops_taskreport',
        tags="ops_taskreport"			
    )			}}	

SELECT
    *
FROM
    {{ ref('policy_cos_clean_insurance') }}