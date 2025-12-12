{{ config(
    alias = 'clientstatus_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('clientstatus_vc_clean_insurance') }}