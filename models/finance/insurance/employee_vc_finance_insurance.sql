{{ config(
    alias = 'employee_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('employee_vc_clean_insurance') }}