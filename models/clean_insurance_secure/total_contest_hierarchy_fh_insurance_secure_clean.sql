{{  config(alias='total_contest_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('total_contest_credits_for_share_clean') }}