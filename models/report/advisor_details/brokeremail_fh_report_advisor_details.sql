{{ config (			
        materialized="view",			
        alias='BrokerEmail', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokeremail_vc_report_insurance') }}
