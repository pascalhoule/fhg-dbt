{{ config(
    alias = 'brokercos_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokercos_vc_clean_insurance') }}