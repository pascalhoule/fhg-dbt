{{ config(
    alias = 'brokerphone_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokerphone_vc_clean_insurance') }}