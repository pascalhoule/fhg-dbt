{{ config(
    alias = 'employee_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
  *
FROM {{ ref('employee_vc_clean_insurance') }}