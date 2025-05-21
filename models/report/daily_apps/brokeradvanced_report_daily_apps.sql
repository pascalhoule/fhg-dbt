{{ config (			
        materialized="view",			
        alias='Brokeradvanced', 			
        database='report', 			
        schema='daily_apps',
        tags="daily_apps"			
    ) }}	


SELECT *
FROM
    {{ ref('brokeradvanced_vc_report_insurance') }} 