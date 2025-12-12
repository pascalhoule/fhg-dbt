{{ config(
    alias = 'brokeraddress_vc', 
    database = 'finance', 
    schema = 'insurance', 
    materialized = "view") }} 

SELECT
    AGENTCODE, 
    TYPE, 
    ADDRESS, 
    CITY, 
    PROVINCE, 
    POSTAL_CODE, 
    COUNTRY
FROM {{ ref('brokeraddress_vc_clean_insurance') }}