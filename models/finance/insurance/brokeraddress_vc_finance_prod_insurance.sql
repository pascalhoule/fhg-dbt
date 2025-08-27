{{ config(
    alias = 'brokeraddress_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialization = "view") }} 

SELECT
    *
FROM {{ ref('brokeraddress_vc_clean_insurance') }}