{{ config(
    alias = 'carrierplan_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('carrierplan_vc_clean_insurance') }}