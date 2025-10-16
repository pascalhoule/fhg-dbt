{{  config(alias='sales_policy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sales_policy_fh_for_share_clean') }}