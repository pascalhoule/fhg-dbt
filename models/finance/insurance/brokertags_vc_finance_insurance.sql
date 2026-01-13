{{ config(
    alias = 'brokertags_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokertags_vc_clean_insurance') }}