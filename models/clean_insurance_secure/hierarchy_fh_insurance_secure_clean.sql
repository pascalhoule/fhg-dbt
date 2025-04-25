{{  config(alias='ins_hierarchy_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT *
FROM {{ ref('hierarchy_fh_tbl_clean_insurance') }}