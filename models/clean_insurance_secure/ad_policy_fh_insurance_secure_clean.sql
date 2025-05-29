{{  config(alias='ad_policy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_policy_fh_for_share_clean') }}