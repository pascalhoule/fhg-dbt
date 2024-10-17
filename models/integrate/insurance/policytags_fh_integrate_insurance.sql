{{			
    config (			
        materialized="view",			
        alias='policytags_fh', 			
        database='integrate', 			
        schema='insurance',
        tags=["in_the_mill"]			
    )			
}}

SELECT * FROM {{ ref('policytags_fh_normalize_insurance') }}
