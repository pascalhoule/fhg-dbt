{{ config(
    alias = 'brokergroupmembership_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokergroupmembership_vc_clean_insurance') }}
