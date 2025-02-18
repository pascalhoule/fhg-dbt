{{			
    config (			
        materialized="view",			
        alias='broker_fh', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps"]			
    )			
}}	

SELECT
    *
FROM
    {{ ref('broker_fh_report_insurance') }}