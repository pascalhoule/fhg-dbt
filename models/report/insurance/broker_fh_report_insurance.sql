{{			
    config (			
        materialized="view",			
        alias='broker_fh', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

 SELECT *
 FROM {{ ref('broker_fh_analyze_insurance') }}