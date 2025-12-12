{{ config(
    alias = 'brokerledgersummary_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('brokerledgersummary_vc_clean_insurance') }}