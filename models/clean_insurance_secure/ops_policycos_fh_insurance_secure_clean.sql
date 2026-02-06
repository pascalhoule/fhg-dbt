{{  config(alias='ops_policycos_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ops_policycos_fh_for_share_clean') }}