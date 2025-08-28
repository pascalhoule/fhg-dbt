{{ config(
    alias = 'client_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('client_vc_clean_insurance') }}