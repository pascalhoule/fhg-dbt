{{ config (			
        materialized="view",			
        alias='BrokerTags', 			
        database='report', 			
        schema='daily_apps',
        tags="adaily_apps"			
    ) }}	


SELECT *
FROM
    {{ ref('brokertags_vc_report_insurance') }}