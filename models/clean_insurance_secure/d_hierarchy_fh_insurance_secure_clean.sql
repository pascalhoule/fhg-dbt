{{  config(alias='ad_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_hierarchy_fh_for_share_clean') }}