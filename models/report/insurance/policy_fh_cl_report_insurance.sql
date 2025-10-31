{{			
    config (			
        materialized="view",			
        alias='policy_fh_cl', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

 SELECT *
 FROM {{ ref('policy_fh_cl_analyze_insurance') }}