{{ config(
    alias = 'hierarchy_fh', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('hierarchy_fh_normalize_insurance') }}