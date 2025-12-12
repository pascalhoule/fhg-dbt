{{ config(
    alias = 'brokerreplink_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokerreplink_vc_clean_insurance') }}