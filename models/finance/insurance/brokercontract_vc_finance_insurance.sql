{{ config(
    alias = 'brokercontract_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
   *
FROM {{ ref('brokercontract_vc_clean_insurance') }}