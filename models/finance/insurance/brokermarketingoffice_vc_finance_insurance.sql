{{ config(
    alias = 'brokermarketingoffice_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
  *
FROM {{ ref('brokermarketingoffice_vc_clean_insurance') }}