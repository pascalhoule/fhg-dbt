{{ config(
    alias = 'commissiongrid_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('comissiongrid_vc_clean_insurance') }}