{{  config(alias='budget_2025_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_budget_2025_fh_for_share_clean') }}