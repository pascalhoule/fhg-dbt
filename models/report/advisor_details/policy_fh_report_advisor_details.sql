{{ config (			
        materialized="view",			
        alias='Policy', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('policy_vc_integrate_insurance') }} 