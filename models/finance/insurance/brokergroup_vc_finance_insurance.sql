{{ config(
    alias = 'brokergroup_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokergroup_vc_clean_insurance') }}