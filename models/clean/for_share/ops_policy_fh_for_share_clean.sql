{{  config(alias='ops_policy_fh', 
database='clean', 
schema='for_share')  }} 

SELECT * FROM {{ ref('policy_vc_clean_insurance') }}