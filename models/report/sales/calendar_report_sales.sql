{{			
    config (			
        materialized="view",			
        alias='calendar', 			
        database='report', 			
        schema='sales'			
    )			
}}	

SELECT *
FROM {{ ref('calendar_report_dimensions') }}