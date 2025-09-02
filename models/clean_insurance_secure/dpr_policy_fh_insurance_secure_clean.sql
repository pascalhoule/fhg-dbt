{{  config(alias='dpr_policy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_policy_fh_for_share_clean') }}