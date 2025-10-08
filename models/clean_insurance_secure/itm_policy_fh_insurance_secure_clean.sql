{{  config(alias='itm_policy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('itm_policy_fh_for_share_clean') }}