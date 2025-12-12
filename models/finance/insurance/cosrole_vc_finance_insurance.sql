{{ config(
    alias = 'cosrole_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('cosrole_vc_clean_insurance') }}