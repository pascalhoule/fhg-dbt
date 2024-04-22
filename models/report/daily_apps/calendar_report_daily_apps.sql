{{			
    config (			
        materialized="view",			
        alias='calendar', 			
        database='report', 			
        schema='daily_apps'			
    )			
}}	

SELECT *
FROM {{ ref('calendar_report_dimensions') }}