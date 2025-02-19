{{ config (			
        materialized="view",			
        alias='BrokerPhone', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokerphone_vc_report_insurance') }}
