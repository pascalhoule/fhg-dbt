{{ config(
    alias = 'brokereo_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokereo_vc_clean_insurance') }}