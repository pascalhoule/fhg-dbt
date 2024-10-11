{{			
    config (			
        materialized="table",			
        alias='policy_fh', 			
        database='normalize', 			
        schema='insurance',
        tags=["policy_fh"]			
    )			
}}

SELECT * FROM {{ ref ('__fix_two_serv_one_comm_normalize_insurance') }}
