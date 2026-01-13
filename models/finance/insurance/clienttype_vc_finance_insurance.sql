{{ config(
    alias = 'clienttype_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('clienttype_vc_clean_insurance') }}