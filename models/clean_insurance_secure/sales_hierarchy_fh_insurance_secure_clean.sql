{{  config(alias='sales_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('sales_hierarchy_fh_for_share_clean') }}