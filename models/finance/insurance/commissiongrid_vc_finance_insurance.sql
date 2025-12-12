{{ config(
    alias = 'commissiongrid_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('commissiongrid_vc_clean_insurance') }}