{{ config(
    alias='brokeraddress_vc', 
    database='agt_comm', 
    schema='insurance') }} 

SELECT *
FROM {{ ref('brokeraddress_vc_analyze_insurance') }}