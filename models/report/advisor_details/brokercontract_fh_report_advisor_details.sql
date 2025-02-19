{{ config (			
        materialized="view",			
        alias='BrokerContract', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokercontract_vc_report_insurance') }}
