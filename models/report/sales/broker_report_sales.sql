{{			
    config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='sales',
        tags=["sales"]			
    )			
}}	

SELECT
    *
FROM
    {{ ref('broker_fh_report_insurance') }}