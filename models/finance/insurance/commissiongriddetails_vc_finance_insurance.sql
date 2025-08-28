{{ config(
    alias = 'commissiongriddetails_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('commissiongriddetails_vc_clean_insurance') }}