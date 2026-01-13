{{ config(
    alias = 'policy_fh', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('policy_fh_normalize_insurance') }}