{{ config (			
        materialized="view",			
        alias='BrokerCOS', 			
        database='report', 			
        schema='sales',
        tags="sales",
   grants = {'ownership': ['FH_READER']})			
     }}	


SELECT *
FROM
    {{ ref('brokercos_vc_report_insurance') }}