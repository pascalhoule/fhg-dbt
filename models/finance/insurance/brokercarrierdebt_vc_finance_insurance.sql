{{ config(
    alias = 'brokercarrierdebt_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
   *
FROM {{ ref('brokercarrierdebt_vc_clean_insurance') }}