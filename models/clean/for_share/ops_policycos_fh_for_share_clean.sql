{{  config(alias='ops_policycos_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_cos_clean_insurance') }}