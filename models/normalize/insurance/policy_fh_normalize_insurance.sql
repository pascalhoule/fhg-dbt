{{			
    config (			
        materialized="table",			
        alias='policy_fh', 			
        database='normalize', 			
        schema='insurance',
        tags=["policy_fh"]			
    )			
}}

SELECT * FROM {{ ref ('__COMM_AGT_MAPPED_CORRECTED_insurance') }}