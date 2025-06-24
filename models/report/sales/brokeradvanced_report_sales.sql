{{ config (			
        materialized="view",			
        alias='brokeradvanced', 			
        database='report', 			
        schema='sales',			
    ) }}	


SELECT *
FROM
    {{ ref('brokeradvanced_vc_report_insurance') }} 