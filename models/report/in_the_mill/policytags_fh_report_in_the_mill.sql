{{			
    config (			
        materialized="view",			
        alias='policytags_fh', 			
        database='report', 			
        schema = 'in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["in_the_mill"]			
    )			
}}

SELECT * FROM {{ ref('policytags_fh_report_insurance') }}