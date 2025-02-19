{{ config (			
        materialized="view",			
        alias='BrokerTags', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('brokertags_vc_report_insurance') }}
