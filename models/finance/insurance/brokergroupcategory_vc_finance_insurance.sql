{{ config(
    alias = 'brokergroupcategory_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokergroupcategory_vc_clean_insurance') }}