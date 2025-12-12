{{ config(
    alias = 'employeerole_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('employeerole_vc_clean_insurance') }}