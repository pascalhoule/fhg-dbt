{{ config(
    alias = 'brokergroupcategory_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokergroupcategory_vc_clean_insurance') }}