{{			
    config (			
        materialized="view",			
        alias='calendar', 			
        database='report', 			
        schema = 'in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["in_the_mill"]			
    )			
}}	

SELECT *
FROM {{ ref('calendar_report_dimensions') }}