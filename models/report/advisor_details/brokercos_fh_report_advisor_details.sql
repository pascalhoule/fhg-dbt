{{ config (			
        materialized="view",			
        alias='BrokerCOS', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokercos_vc_report_insurance') }}
