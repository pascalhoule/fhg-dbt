{{ config (			
        materialized="view",			
        alias='BrokerCOS', 			
        database='report', 			
        schema='sales',
        tags="sales"			
    ) }}	


SELECT *
FROM
    {{ ref('brokercos_vc_report_insurance') }}