{{ config(
    alias = 'commissiongrid_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('commissiongrid_vc_clean_insurance') }}