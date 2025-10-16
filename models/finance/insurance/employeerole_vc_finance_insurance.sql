{{ config(
    alias = 'employeerole_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('employeerole_vc_clean_insurance') }}