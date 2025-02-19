{{ config (			
        materialized="view",			
        alias='Firms', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('firms_fh_report_insurance') }}
