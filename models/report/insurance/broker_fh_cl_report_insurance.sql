{{			
    config (			
        materialized="view",			
        alias='broker_fh_cl', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

 SELECT *
 FROM {{ ref('broker_fh_cl_analyze_insurance') }}