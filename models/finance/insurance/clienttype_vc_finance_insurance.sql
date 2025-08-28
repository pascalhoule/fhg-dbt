{{ config(
    alias = 'clienttype_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('clienttype_vc_clean_insurance') }}