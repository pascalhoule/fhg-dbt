{{			
    config (			
        materialized="view",			
        alias='broker_fh_cl', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref('broker_fh_cl_integrate_insurance') }}