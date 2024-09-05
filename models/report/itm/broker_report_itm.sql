{{			
    config (			
        materialized="view",			
        alias='broker', 			
        database='report', 			
        schema='itm'			
    )			
}}	

SELECT
    *
FROM
    {{ ref ('broker_fh_report_insurance') }}