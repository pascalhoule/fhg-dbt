{{  config(alias='total_contest_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('total_contest_hierarchy_fh_for_share_clean') }}