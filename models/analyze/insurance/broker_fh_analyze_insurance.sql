{{			
    config (			
        materialized="view",			
        alias='broker_fh', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref('broker_fh_integrate_insurance') }}
