{{			
    config (			
        materialized="table",			
        alias='broker_fh', 			
        database='clean', 			
        schema='insurance'			
    )			
}}

SELECT 
* 
FROM {{ ref('broker_fh_integrate_insurance') }}