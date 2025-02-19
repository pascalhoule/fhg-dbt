{{ config (			
        materialized="view",			
        alias='Brokeradvanced', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokeradvanced_vc_report_insurance') }}
