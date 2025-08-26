{{			
    config (			
        materialized="view",			
        alias='daily_insurance_policy_cl_detail', 			
        database='integrate', 			
        schema='insurance',
        grants = {'ownership': ['FH_READER']},			
    )			
}}

select * FROM

{{ source('acdirect_sandbox', 'daily_insurance_ac_direct_agreement') }}