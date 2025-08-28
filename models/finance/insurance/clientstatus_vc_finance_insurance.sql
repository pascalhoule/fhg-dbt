{{ config(
    alias = 'clientstatus_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('clientstatus_vc_clean_insurance') }}