{{  config(alias='dpr_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_hierarchy_fh_for_share_clean') }}