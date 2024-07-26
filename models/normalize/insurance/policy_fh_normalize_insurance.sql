{{			
    config (			
        materialized="table",			
        alias='policy_fh', 			
        database='normalize', 			
        schema='insurance',
        tags=["policy_fh"]			
    )			
}}

SELECT * FROM {{ ref ('__FREEZE_APPDT_insurance') }}
