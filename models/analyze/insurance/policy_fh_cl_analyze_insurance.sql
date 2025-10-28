{{			
    config (			
        materialized="view",			
        alias='policy_fh_cl', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref('policy_fh_cl_integrate_insurance') }}