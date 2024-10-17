{{			
    config (			
        materialized="view",			
        alias='policytags_fh', 			
        database='report', 			
        schema='insurance',
        tags=["in_the_mill"]			
    )			
}}

SELECT * FROM {{ ref('policytags_fh_analyze_insurance') }}