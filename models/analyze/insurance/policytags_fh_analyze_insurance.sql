{{			
    config (			
        materialized="view",			
        alias='policytags_fh', 			
        database='analyze', 			
        schema='insurance',
        tags=["in_the_mill"]			
    )			
}}

SELECT * FROM {{ ref('policytags_fh_integrate_insurance') }}