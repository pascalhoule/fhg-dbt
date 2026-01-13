{{ config(
    alias = 'brokertasks_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokertasks_vc_clean_insurance') }}