{{ config(
    alias = 'hierarchy_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('hierarchy_vc_clean_insurance') }}