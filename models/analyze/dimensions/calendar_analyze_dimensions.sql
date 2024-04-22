{{			
    config (			
        materialized="view",			
        alias='calendar', 			
        database='analyze', 			
        schema='dimensions'			
    )			
}}	

SELECT *
FROM {{ ref('calendar') }}