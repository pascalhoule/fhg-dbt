{{ config (			
        materialized="view",			
        alias='Brokeradvanced', 			
        database='report', 			
        schema='in_the_mill'
        			
    ) }}	


SELECT *
FROM
    {{ ref('brokeradvanced_vc_report_insurance') }} 