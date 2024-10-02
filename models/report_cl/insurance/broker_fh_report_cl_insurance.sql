{{			
    config (			
        materialized="view",			
        alias='broker_fh', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

 SELECT *
 FROM {{ ref('broker_fh_analyze_insurance') }}