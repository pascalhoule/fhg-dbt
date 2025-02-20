{{ config (			
        materialized="view",			
        alias='Policy', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('policy_fh_report_insurance') }} 