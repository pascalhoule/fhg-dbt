{{			
    config (			
        materialized="view",			
        alias='calendar', 			
        database='report', 			
        schema='dimensions'			
    )			
}}	

SELECT *
FROM {{ ref('calendar_analyze_dimensions') }}