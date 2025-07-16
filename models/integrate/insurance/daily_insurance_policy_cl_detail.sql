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
{{ source('acdirect_policy', 'daily_insurance_AC_Direct_agreement_20250716') }}