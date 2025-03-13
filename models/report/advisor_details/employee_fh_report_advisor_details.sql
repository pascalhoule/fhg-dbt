{{ config (			
        materialized="view",			
        alias='Employee', 			
        database='report', 			
        schema='advisor_details',
        tags="advisor_details"			
    ) }}	


SELECT *
FROM
    {{ ref('employee_vc_report_insurance') }}
