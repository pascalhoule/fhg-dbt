{{ config(
    alias = 'policy_fh', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('policy_fh_normalize_insurance') }}