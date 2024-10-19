{{			
    config (			
        materialized = "view",			
        alias = 'broker', 			
        database = 'report', 			
        schema = 'in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["in_the_mill"]			
    )			
}}	

SELECT
    *
FROM
    {{ ref('broker_fh_report_insurance') }}