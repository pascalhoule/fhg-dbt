{{ config(
    alias = 'hierarchy_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('hierarchy_vc_clean_insurance') }}