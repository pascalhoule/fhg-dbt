{{ config (			
        materialized="view",			
        alias='BrokerTags', 			
        database='report', 			
        schema='sales',
     	
    ) }}	


SELECT *
FROM
    {{ ref('brokertags_vc_report_insurance') }}