{{ config(
    alias = 'brokercos_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokercos_vc_clean_insurance') }}