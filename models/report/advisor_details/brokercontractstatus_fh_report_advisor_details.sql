{{ config (			
        materialized="view",			
        alias='BrokerContractStatus', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokercontractstatus_vc_report_insurance') }}
