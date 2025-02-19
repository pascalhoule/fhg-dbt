{{ config (			
        materialized="view",			
        alias='BrokerAddress', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokeraddress_vc_report_insurance') }}
